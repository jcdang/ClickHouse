#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Processors/Formats/Impl/TabSeparatedRowInputFormat.h>
#include <Formats/verbosePrintString.h>
#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}


static void skipTSVRow(ReadBuffer & istr, const size_t num_columns)
{
    NullSink null_sink;

    for (size_t i = 0; i < num_columns; ++i)
    {
        readEscapedStringInto(null_sink, istr);
        assertChar(i == num_columns - 1 ? '\n' : '\t', istr);
    }
}


/** Check for a common error case - usage of Windows line feed.
  */
static void checkForCarriageReturn(ReadBuffer & istr)
{
    if (istr.position()[0] == '\r' || (istr.position() != istr.buffer().begin() && istr.position()[-1] == '\r'))
        throw Exception("\nYou have carriage return (\\r, 0x0D, ASCII 13) at end of first row."
            "\nIt's like your input data has DOS/Windows style line separators, that are illegal in TabSeparated format."
            " You must transform your file to Unix format."
            "\nBut if you really need carriage return at end of string value of last column, you need to escape it as \\r.",
            ErrorCodes::INCORRECT_DATA);
}


TabSeparatedRowInputFormat::TabSeparatedRowInputFormat(
    ReadBuffer & in_, Block header_, bool with_names_, bool with_types_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(std::move(header_), in_, std::move(params_)), with_names(with_names_), with_types(with_types_), format_settings(format_settings_)
{
    auto & sample = getPort().getHeader();
    size_t num_columns = sample.columns();

    data_types.resize(num_columns);
    column_indexes_by_names.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column_info = sample.getByPosition(i);

        data_types[i] = column_info.type;
        column_indexes_by_names.emplace(column_info.name, i);
    }

    column_indexes_for_input_fields.reserve(num_columns);
    read_columns.assign(num_columns, false);
}


void TabSeparatedRowInputFormat::setupAllColumnsByTableSchema()
{
    auto & header = getPort().getHeader();
    read_columns.assign(header.columns(), true);
    column_indexes_for_input_fields.resize(header.columns());

    for (size_t i = 0; i < column_indexes_for_input_fields.size(); ++i)
        column_indexes_for_input_fields[i] = i;
}


void TabSeparatedRowInputFormat::addInputColumn(const String & column_name)
{
    const auto column_it = column_indexes_by_names.find(column_name);
    if (column_it == column_indexes_by_names.end())
    {
        if (format_settings.skip_unknown_fields)
        {
            column_indexes_for_input_fields.push_back(std::nullopt);
            return;
        }

        throw Exception(
                "Unknown field found in TSV header: '" + column_name + "' " +
                "at position " + std::to_string(column_indexes_for_input_fields.size()) +
                "\nSet the 'input_format_skip_unknown_fields' parameter explicitly to ignore and proceed",
                ErrorCodes::INCORRECT_DATA
        );
    }

    const auto column_index = column_it->second;

    if (read_columns[column_index])
        throw Exception("Duplicate field found while parsing TSV header: " + column_name, ErrorCodes::INCORRECT_DATA);

    read_columns[column_index] = true;
    column_indexes_for_input_fields.emplace_back(column_index);
}


void TabSeparatedRowInputFormat::fillUnreadColumnsWithDefaults(MutableColumns & columns, RowReadExtension & row_read_extension)
{
    /// It is safe to memorize this on the first run - the format guarantees this does not change
    if (unlikely(row_num == 1))
    {
        columns_to_fill_with_default_values.clear();
        for (size_t index = 0; index < read_columns.size(); ++index)
            if (read_columns[index] == 0)
                columns_to_fill_with_default_values.push_back(index);
    }

    for (const auto column_index : columns_to_fill_with_default_values)
        data_types[column_index]->insertDefaultInto(*columns[column_index]);

    row_read_extension.read_columns = read_columns;
}


void TabSeparatedRowInputFormat::readPrefix()
{
    if (with_names || with_types)
    {
        /// In this format, we assume that column name or type cannot contain BOM,
        ///  so, if format has header,
        ///  then BOM at beginning of stream cannot be confused with name or type of field, and it is safe to skip it.
        skipBOMIfExists(in);
    }

    if (with_names)
    {
        if (format_settings.with_names_use_header)
        {
            String column_name;
            do
            {
                readEscapedString(column_name, in);
                addInputColumn(column_name);
            }
            while (checkChar('\t', in));

            if (!in.eof())
            {
                checkForCarriageReturn(in);
                assertChar('\n', in);
            }
        }
        else
        {
            setupAllColumnsByTableSchema();
            skipTSVRow(in, column_indexes_for_input_fields.size());
        }
    }
    else
        setupAllColumnsByTableSchema();

    if (with_types)
    {
        skipTSVRow(in, column_indexes_for_input_fields.size());
    }
}


bool TabSeparatedRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (in.eof())
        return false;

    updateDiagnosticInfo();

    for (size_t input_position = 0; input_position < column_indexes_for_input_fields.size(); ++input_position)
    {
        const auto & column_index = column_indexes_for_input_fields[input_position];
        if (column_index)
        {
            data_types[*column_index]->deserializeAsTextEscaped(*columns[*column_index], in, format_settings);
        }
        else
        {
            NullSink null_sink;
            readEscapedStringInto(null_sink, in);
        }

        /// skip separators
        if (input_position + 1 < column_indexes_for_input_fields.size())
        {
            assertChar('\t', in);
        }
        else if (!in.eof())
        {
            if (unlikely(row_num == 1))
                checkForCarriageReturn(in);

            assertChar('\n', in);
        }
    }

    fillUnreadColumnsWithDefaults(columns, ext);

    return true;
}


String TabSeparatedRowInputFormat::getDiagnosticInfo()
{
    if (in.eof())        /// Buffer has gone, cannot extract information about what has been parsed.
        return {};

    auto & header = getPort().getHeader();
    WriteBufferFromOwnString out;
    MutableColumns columns = header.cloneEmptyColumns();

    /// It is possible to display detailed diagnostics only if the last and next to last lines are still in the read buffer.
    size_t bytes_read_at_start_of_buffer = in.count() - in.offset();
    if (bytes_read_at_start_of_buffer != bytes_read_at_start_of_buffer_on_prev_row)
    {
        out << "Could not print diagnostic info because two last rows aren't in buffer (rare case)\n";
        return out.str();
    }

    size_t max_length_of_column_name = 0;
    for (size_t i = 0; i < header.columns(); ++i)
        if (header.safeGetByPosition(i).name.size() > max_length_of_column_name)
            max_length_of_column_name = header.safeGetByPosition(i).name.size();

    size_t max_length_of_data_type_name = 0;
    for (size_t i = 0; i < header.columns(); ++i)
        if (header.safeGetByPosition(i).type->getName().size() > max_length_of_data_type_name)
            max_length_of_data_type_name = header.safeGetByPosition(i).type->getName().size();

    /// Roll back the cursor to the beginning of the previous or current line and pars all over again. But now we derive detailed information.

    if (pos_of_prev_row)
    {
        in.position() = pos_of_prev_row;

        out << "\nRow " << (row_num - 1) << ":\n";
        if (!parseRowAndPrintDiagnosticInfo(columns, out, max_length_of_column_name, max_length_of_data_type_name))
            return out.str();
    }
    else
    {
        if (!pos_of_current_row)
        {
            out << "Could not print diagnostic info because parsing of data hasn't started.\n";
            return out.str();
        }

        in.position() = pos_of_current_row;
    }

    out << "\nRow " << row_num << ":\n";
    parseRowAndPrintDiagnosticInfo(columns, out, max_length_of_column_name, max_length_of_data_type_name);
    out << "\n";

    return out.str();
}


bool TabSeparatedRowInputFormat::parseRowAndPrintDiagnosticInfo(MutableColumns & columns,
    WriteBuffer & out, size_t max_length_of_column_name, size_t max_length_of_data_type_name)
{
    for (size_t input_position = 0; input_position < column_indexes_for_input_fields.size(); ++input_position)
    {
        if (input_position == 0 && in.eof())
        {
            out << "<End of stream>\n";
            return false;
        }

        if (column_indexes_for_input_fields[input_position].has_value())
        {
            const auto & column_index = *column_indexes_for_input_fields[input_position];
            const auto & current_column_type = data_types[column_index];

            const auto & header = getPort().getHeader();

            out << "Column " << input_position << ", " << std::string((input_position < 10 ? 2 : input_position < 100 ? 1 : 0), ' ')
                << "name: " << header.safeGetByPosition(column_index).name << ", " << std::string(max_length_of_column_name - header.safeGetByPosition(column_index).name.size(), ' ')
                << "type: " << current_column_type->getName() << ", " << std::string(max_length_of_data_type_name - current_column_type->getName().size(), ' ');

            auto prev_position = in.position();
            std::exception_ptr exception;

            try
            {
                current_column_type->deserializeAsTextEscaped(*columns[column_index], in, format_settings);
            }
            catch (...)
            {
                exception = std::current_exception();
            }

            auto curr_position = in.position();

            if (curr_position < prev_position)
                throw Exception("Logical error: parsing is non-deterministic.", ErrorCodes::LOGICAL_ERROR);

            if (isNativeNumber(current_column_type) || isDateOrDateTime(current_column_type))
            {
                /// An empty string instead of a value.
                if (curr_position == prev_position)
                {
                    out << "ERROR: text ";
                    verbosePrintString(prev_position, std::min(prev_position + 10, in.buffer().end()), out);
                    out << " is not like " << current_column_type->getName() << "\n";
                    return false;
                }
            }

            out << "parsed text: ";
            verbosePrintString(prev_position, curr_position, out);

            if (exception)
            {
                if (current_column_type->getName() == "DateTime")
                    out << "ERROR: DateTime must be in YYYY-MM-DD hh:mm:ss or NNNNNNNNNN (unix timestamp, exactly 10 digits) format.\n";
                else if (current_column_type->getName() == "Date")
                    out << "ERROR: Date must be in YYYY-MM-DD format.\n";
                else
                    out << "ERROR\n";
                return false;
            }

            out << "\n";

            if (current_column_type->haveMaximumSizeOfValue())
            {
                if (*curr_position != '\n' && *curr_position != '\t')
                {
                    out << "ERROR: garbage after " << current_column_type->getName() << ": ";
                    verbosePrintString(curr_position, std::min(curr_position + 10, in.buffer().end()), out);
                    out << "\n";

                    if (current_column_type->getName() == "DateTime")
                        out << "ERROR: DateTime must be in YYYY-MM-DD hh:mm:ss or NNNNNNNNNN (unix timestamp, exactly 10 digits) format.\n";
                    else if (current_column_type->getName() == "Date")
                        out << "ERROR: Date must be in YYYY-MM-DD format.\n";

                    return false;
                }
            }
        }
        else
        {
            static const String skipped_column_str = "<SKIPPED COLUMN>";
            out << "Column " << input_position << ", " << std::string((input_position < 10 ? 2 : input_position < 100 ? 1 : 0), ' ')
                << "name: " << skipped_column_str << ", " << std::string(max_length_of_column_name - skipped_column_str.length(), ' ')
                << "type: " << skipped_column_str << ", " << std::string(max_length_of_data_type_name - skipped_column_str.length(), ' ');

            NullSink null_sink;
            readEscapedStringInto(null_sink, in);
        }

        /// Delimiters
        if (input_position + 1 == column_indexes_for_input_fields.size())
        {
            if (!in.eof())
            {
                try
                {
                    assertChar('\n', in);
                }
                catch (const DB::Exception &)
                {
                    if (*in.position() == '\t')
                    {
                        out << "ERROR: Tab found where line feed is expected."
                               " It's like your file has more columns than expected.\n"
                               "And if your file have right number of columns, maybe it have unescaped tab in value.\n";
                    }
                    else if (*in.position() == '\r')
                    {
                        out << "ERROR: Carriage return found where line feed is expected."
                               " It's like your file has DOS/Windows style line separators, that is illegal in TabSeparated format.\n";
                    }
                    else
                    {
                        out << "ERROR: There is no line feed. ";
                        verbosePrintString(in.position(), in.position() + 1, out);
                        out << " found instead.\n";
                    }
                    return false;
                }
            }
        }
        else
        {
            try
            {
                assertChar('\t', in);
            }
            catch (const DB::Exception &)
            {
                if (*in.position() == '\n')
                {
                    out << "ERROR: Line feed found where tab is expected."
                           " It's like your file has less columns than expected.\n"
                           "And if your file have right number of columns, maybe it have unescaped backslash in value before tab, which cause tab has escaped.\n";
                }
                else if (*in.position() == '\r')
                {
                    out << "ERROR: Carriage return found where tab is expected.\n";
                }
                else
                {
                    out << "ERROR: There is no tab. ";
                    verbosePrintString(in.position(), in.position() + 1, out);
                    out << " found instead.\n";
                }
                return false;
            }
        }
    }

    return true;
}


void TabSeparatedRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(in);
}


void TabSeparatedRowInputFormat::updateDiagnosticInfo()
{
    ++row_num;

    bytes_read_at_start_of_buffer_on_prev_row = bytes_read_at_start_of_buffer_on_current_row;
    bytes_read_at_start_of_buffer_on_current_row = in.count() - in.offset();

    pos_of_prev_row = pos_of_current_row;
    pos_of_current_row = in.position();
}


void registerInputFormatProcessorTabSeparated(FormatFactory & factory)
{
    for (auto name : {"TabSeparated", "TSV"})
    {
        factory.registerInputFormatProcessor(name, [](
            ReadBuffer & buf,
            const Block & sample,
            const Context &,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<TabSeparatedRowInputFormat>(buf, sample, false, false, std::move(params), settings);
        });
    }

    for (auto name : {"TabSeparatedWithNames", "TSVWithNames"})
    {
        factory.registerInputFormatProcessor(name, [](
            ReadBuffer & buf,
            const Block & sample,
            const Context &,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<TabSeparatedRowInputFormat>(buf, sample, true, false, std::move(params), settings);
        });
    }

    for (auto name : {"TabSeparatedWithNamesAndTypes", "TSVWithNamesAndTypes"})
    {
        factory.registerInputFormatProcessor(name, [](
            ReadBuffer & buf,
            const Block & sample,
            const Context &,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<TabSeparatedRowInputFormat>(buf, sample, true, true, std::move(params), settings);
        });
    }
}

}
