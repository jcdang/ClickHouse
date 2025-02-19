#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_MUTATION_COMMAND;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int CANNOT_UPDATE_COLUMN;
}

bool MutationsInterpreter::isStorageTouchedByMutations() const
{
    if (commands.empty())
        return false;

    for (const MutationCommand & command : commands)
    {
        if (!command.predicate) /// The command touches all rows.
            return true;
    }

    /// Execute `SELECT count() FROM storage WHERE predicate1 OR predicate2 OR ...` query.
    /// The result can differ from tne number of affected rows (e.g. if there is an UPDATE command that
    /// changes how many rows satisfy the predicates of the subsequent commands).
    /// But we can be sure that if count = 0, then no rows will be touched.

    auto select = std::make_shared<ASTSelectQuery>();

    select->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    auto count_func = std::make_shared<ASTFunction>();
    count_func->name = "count";
    count_func->arguments = std::make_shared<ASTExpressionList>();
    select->select()->children.push_back(count_func);

    if (commands.size() == 1)
        select->setExpression(ASTSelectQuery::Expression::WHERE, commands[0].predicate->clone());
    else
    {
        auto coalesced_predicates = std::make_shared<ASTFunction>();
        coalesced_predicates->name = "or";
        coalesced_predicates->arguments = std::make_shared<ASTExpressionList>();
        coalesced_predicates->children.push_back(coalesced_predicates->arguments);

        for (const MutationCommand & command : commands)
            coalesced_predicates->arguments->children.push_back(command.predicate->clone());

        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(coalesced_predicates));
    }

    auto context_copy = context;
    context_copy.getSettingsRef().merge_tree_uniform_read_distribution = 0;
    context_copy.getSettingsRef().max_threads = 1;

    BlockInputStreamPtr in = InterpreterSelectQuery(select, context_copy, storage, SelectQueryOptions().ignoreLimits()).execute().in;

    Block block = in->read();
    if (!block.rows())
        return false;
    else if (block.rows() != 1)
        throw Exception("count() expression returned " + toString(block.rows()) + " rows, not 1",
            ErrorCodes::LOGICAL_ERROR);

    auto count = (*block.getByName("count()").column)[0].get<UInt64>();
    return count != 0;
}


static NameSet getKeyColumns(const StoragePtr & storage)
{
    const MergeTreeData * merge_tree_data = dynamic_cast<const MergeTreeData *>(storage.get());
    if (!merge_tree_data)
        return {};

    NameSet key_columns;

    if (merge_tree_data->partition_key_expr)
        for (const String & col : merge_tree_data->partition_key_expr->getRequiredColumns())
            key_columns.insert(col);

    auto sorting_key_expr = merge_tree_data->sorting_key_expr;
    if (sorting_key_expr)
        for (const String & col : sorting_key_expr->getRequiredColumns())
            key_columns.insert(col);
    /// We don't process sample_by_ast separately because it must be among the primary key columns.

    if (!merge_tree_data->merging_params.sign_column.empty())
        key_columns.insert(merge_tree_data->merging_params.sign_column);

    if (!merge_tree_data->merging_params.version_column.empty())
        key_columns.insert(merge_tree_data->merging_params.version_column);

    return key_columns;
}

static void validateUpdateColumns(
    const StoragePtr & storage, const NameSet & updated_columns,
    const std::unordered_map<String, Names> & column_to_affected_materialized)
{
    NameSet key_columns = getKeyColumns(storage);

    for (const String & column_name : updated_columns)
    {
        auto found = false;
        for (const auto & col : storage->getColumns().getOrdinary())
        {
            if (col.name == column_name)
            {
                found = true;
                break;
            }
        }

        if (!found)
        {
            for (const auto & col : storage->getColumns().getMaterialized())
            {
                if (col.name == column_name)
                    throw Exception("Cannot UPDATE materialized column " + backQuote(column_name), ErrorCodes::CANNOT_UPDATE_COLUMN);
            }

            throw Exception("There is no column " + backQuote(column_name) + " in table", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
        }

        if (key_columns.count(column_name))
            throw Exception("Cannot UPDATE key column " + backQuote(column_name), ErrorCodes::CANNOT_UPDATE_COLUMN);

        auto materialized_it = column_to_affected_materialized.find(column_name);
        if (materialized_it != column_to_affected_materialized.end())
        {
            for (const String & materialized : materialized_it->second)
            {
                if (key_columns.count(materialized))
                    throw Exception("Updated column " + backQuote(column_name) + " affects MATERIALIZED column "
                        + backQuote(materialized) + ", which is a key column. Cannot UPDATE it.",
                        ErrorCodes::CANNOT_UPDATE_COLUMN);
            }
        }
    }
}


void MutationsInterpreter::prepare(bool dry_run)
{
    if (is_prepared)
        throw Exception("MutationsInterpreter is already prepared. It is a bug.", ErrorCodes::LOGICAL_ERROR);

    if (commands.empty())
        throw Exception("Empty mutation commands list", ErrorCodes::LOGICAL_ERROR);

    const ColumnsDescription & columns_desc = storage->getColumns();
    const IndicesDescription & indices_desc = storage->getIndices();
    NamesAndTypesList all_columns = columns_desc.getAllPhysical();

    NameSet updated_columns;
    for (const MutationCommand & command : commands)
    {
        for (const auto & kv : command.column_to_update_expression)
            updated_columns.insert(kv.first);
    }

    /// We need to know which columns affect which MATERIALIZED columns and data skipping indices
    /// to recalculate them if dependencies are updated.
    std::unordered_map<String, Names> column_to_affected_materialized;
    NameSet affected_indices_columns;
    if (!updated_columns.empty())
    {
        for (const auto & column : columns_desc)
        {
            if (column.default_desc.kind == ColumnDefaultKind::Materialized)
            {
                auto query = column.default_desc.expression->clone();
                auto syntax_result = SyntaxAnalyzer(context).analyze(query, all_columns);
                for (const String & dependency : syntax_result->requiredSourceColumns())
                {
                    if (updated_columns.count(dependency))
                        column_to_affected_materialized[dependency].push_back(column.name);
                }
            }
        }
        for (const auto & index : indices_desc.indices)
        {
            auto query = index->expr->clone();
            auto syntax_result = SyntaxAnalyzer(context).analyze(query, all_columns);
            const auto required_columns = syntax_result->requiredSourceColumns();

            for (const String & dependency : required_columns)
            {
                if (updated_columns.count(dependency))
                {
                    affected_indices_columns.insert(std::cbegin(required_columns), std::cend(required_columns));
                    break;
                }
            }
        }

        validateUpdateColumns(storage, updated_columns, column_to_affected_materialized);
    }

    /// First, break a sequence of commands into stages.
    for (const auto & command : commands)
    {
        if (command.type == MutationCommand::DELETE)
        {
            if (stages.empty() || !stages.back().column_to_updated.empty())
                stages.emplace_back(context);

            auto negated_predicate = makeASTFunction("not", command.predicate->clone());
            stages.back().filters.push_back(negated_predicate);
        }
        else if (command.type == MutationCommand::UPDATE)
        {
            if (stages.empty() || !stages.back().column_to_updated.empty())
                stages.emplace_back(context);
            if (stages.size() == 1) /// First stage only supports filtering and can't update columns.
                stages.emplace_back(context);

            NameSet affected_materialized;

            for (const auto & kv : command.column_to_update_expression)
            {
                const String & column = kv.first;

                auto materialized_it = column_to_affected_materialized.find(column);
                if (materialized_it != column_to_affected_materialized.end())
                {
                    for (const String & mat_column : materialized_it->second)
                        affected_materialized.emplace(mat_column);
                }

                const auto & update_expr = kv.second;
                auto updated_column = makeASTFunction("CAST",
                    makeASTFunction("if",
                        command.predicate->clone(),
                        update_expr->clone(),
                        std::make_shared<ASTIdentifier>(column)),
                    std::make_shared<ASTLiteral>(columns_desc.getPhysical(column).type->getName()));
                stages.back().column_to_updated.emplace(column, updated_column);
            }

            if (!affected_materialized.empty())
            {
                stages.emplace_back(context);
                for (const auto & column : columns_desc)
                {
                    if (column.default_desc.kind == ColumnDefaultKind::Materialized)
                    {
                        stages.back().column_to_updated.emplace(
                            column.name,
                            column.default_desc.expression->clone());
                    }
                }
            }
        }
        else if (command.type == MutationCommand::MATERIALIZE_INDEX)
        {
            auto it = std::find_if(
                    std::cbegin(indices_desc.indices), std::end(indices_desc.indices),
                    [&](const std::shared_ptr<ASTIndexDeclaration> & index)
                    {
                        return index->name == command.index_name;
                    });
            if (it == std::cend(indices_desc.indices))
                throw Exception("Unknown index: " + command.index_name, ErrorCodes::BAD_ARGUMENTS);

            auto query = (*it)->expr->clone();
            auto syntax_result = SyntaxAnalyzer(context).analyze(query, all_columns);
            const auto required_columns = syntax_result->requiredSourceColumns();
            affected_indices_columns.insert(std::cbegin(required_columns), std::cend(required_columns));
        }
        else
            throw Exception("Unknown mutation command type: " + DB::toString<int>(command.type), ErrorCodes::UNKNOWN_MUTATION_COMMAND);
    }

    if (!affected_indices_columns.empty())
    {
        if (!stages.empty())
        {
            std::vector<Stage> stages_copy;
            /// Copy all filled stages except index calculation stage.
            for (const auto &stage : stages)
            {
                stages_copy.emplace_back(context);
                stages_copy.back().column_to_updated = stage.column_to_updated;
                stages_copy.back().output_columns = stage.output_columns;
                stages_copy.back().filters = stage.filters;
            }
            auto first_stage_header = prepareInterpreterSelect(stages_copy, /* dry_run = */ true)->getSampleBlock();
            auto in = std::make_shared<NullBlockInputStream>(first_stage_header);
            updated_header = std::make_unique<Block>(addStreamsForLaterStages(stages_copy, in)->getHeader());
        }
        /// Special step to recalculate affected indices.
        stages.emplace_back(context);
        for (const auto & column : affected_indices_columns)
            stages.back().column_to_updated.emplace(
                    column, std::make_shared<ASTIdentifier>(column));
    }

    interpreter_select = prepareInterpreterSelect(stages, dry_run);

    is_prepared = true;
}

std::unique_ptr<InterpreterSelectQuery> MutationsInterpreter::prepareInterpreterSelect(std::vector<Stage> & prepared_stages, bool dry_run)
{
    NamesAndTypesList all_columns = storage->getColumns().getAllPhysical();

    /// Next, for each stage calculate columns changed by this and previous stages.
    for (size_t i = 0; i < prepared_stages.size(); ++i)
    {
        if (!prepared_stages[i].filters.empty())
        {
            for (const auto & column : all_columns)
                prepared_stages[i].output_columns.insert(column.name);
            continue;
        }

        if (i > 0)
            prepared_stages[i].output_columns = prepared_stages[i - 1].output_columns;

        if (prepared_stages[i].output_columns.size() < all_columns.size())
        {
            for (const auto & kv : prepared_stages[i].column_to_updated)
                prepared_stages[i].output_columns.insert(kv.first);
        }
    }

    /// Now, calculate `expressions_chain` for each stage except the first.
    /// Do it backwards to propagate information about columns required as input for a stage to the previous stage.
    for (size_t i = prepared_stages.size() - 1; i > 0; --i)
    {
        auto & stage = prepared_stages[i];

        ASTPtr all_asts = std::make_shared<ASTExpressionList>();

        for (const auto & ast : stage.filters)
            all_asts->children.push_back(ast);

        for (const auto & kv : stage.column_to_updated)
            all_asts->children.push_back(kv.second);

        /// Add all output columns to prevent ExpressionAnalyzer from deleting them from source columns.
        for (const String & column : stage.output_columns)
            all_asts->children.push_back(std::make_shared<ASTIdentifier>(column));

        auto syntax_result = SyntaxAnalyzer(context).analyze(all_asts, all_columns);
        stage.analyzer = std::make_unique<ExpressionAnalyzer>(all_asts, syntax_result, context);

        ExpressionActionsChain & actions_chain = stage.expressions_chain;

        for (const auto & ast : stage.filters)
        {
            if (!actions_chain.steps.empty())
                actions_chain.addStep();
            stage.analyzer->appendExpression(actions_chain, ast, dry_run);
            stage.filter_column_names.push_back(ast->getColumnName());
        }

        if (!stage.column_to_updated.empty())
        {
            if (!actions_chain.steps.empty())
                actions_chain.addStep();

            for (const auto & kv : stage.column_to_updated)
                stage.analyzer->appendExpression(actions_chain, kv.second, dry_run);

            for (const auto & kv : stage.column_to_updated)
            {
                actions_chain.getLastActions()->add(ExpressionAction::copyColumn(
                        kv.second->getColumnName(), kv.first, /* can_replace = */ true));
            }
        }

        /// Remove all intermediate columns.
        actions_chain.addStep();
        actions_chain.getLastStep().required_output.assign(stage.output_columns.begin(), stage.output_columns.end());

        actions_chain.finalize();

        /// Propagate information about columns needed as input.
        for (const auto & column : actions_chain.steps.front().actions->getRequiredColumnsWithTypes())
            prepared_stages[i - 1].output_columns.insert(column.name);
    }

    /// Execute first stage as a SELECT statement.

    auto select = std::make_shared<ASTSelectQuery>();

    select->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    for (const auto & column_name : prepared_stages[0].output_columns)
        select->select()->children.push_back(std::make_shared<ASTIdentifier>(column_name));

    if (!prepared_stages[0].filters.empty())
    {
        ASTPtr where_expression;
        if (prepared_stages[0].filters.size() == 1)
            where_expression = prepared_stages[0].filters[0];
        else
        {
            auto coalesced_predicates = std::make_shared<ASTFunction>();
            coalesced_predicates->name = "and";
            coalesced_predicates->arguments = std::make_shared<ASTExpressionList>();
            coalesced_predicates->children.push_back(coalesced_predicates->arguments);
            coalesced_predicates->arguments->children = prepared_stages[0].filters;
            where_expression = std::move(coalesced_predicates);
        }
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));
    }

    return std::make_unique<InterpreterSelectQuery>(select, context, storage, SelectQueryOptions().analyze(dry_run).ignoreLimits());
}

BlockInputStreamPtr MutationsInterpreter::addStreamsForLaterStages(const std::vector<Stage> & prepared_stages, BlockInputStreamPtr in) const
{
    for (size_t i_stage = 1; i_stage < prepared_stages.size(); ++i_stage)
    {
        const Stage & stage = prepared_stages[i_stage];

        for (size_t i = 0; i < stage.expressions_chain.steps.size(); ++i)
        {
            const auto & step = stage.expressions_chain.steps[i];
            if (i < stage.filter_column_names.size())
            {
                /// Execute DELETEs.
                in = std::make_shared<FilterBlockInputStream>(in, step.actions, stage.filter_column_names[i]);
            }
            else
            {
                /// Execute UPDATE or final projection.
                in = std::make_shared<ExpressionBlockInputStream>(in, step.actions);
            }
        }

        const SubqueriesForSets & subqueries_for_sets = stage.analyzer->getSubqueriesForSets();
        if (!subqueries_for_sets.empty())
            in = std::make_shared<CreatingSetsBlockInputStream>(in, subqueries_for_sets, context);
    }

    in = std::make_shared<MaterializingBlockInputStream>(in);

    return in;
}

void MutationsInterpreter::validate(TableStructureReadLockHolder &)
{
    prepare(/* dry_run = */ true);
    /// Do not use getSampleBlock in order to check the whole pipeline.
    Block first_stage_header = interpreter_select->execute().in->getHeader();
    BlockInputStreamPtr in = std::make_shared<NullBlockInputStream>(first_stage_header);
    addStreamsForLaterStages(stages, in)->getHeader();
}

BlockInputStreamPtr MutationsInterpreter::execute(TableStructureReadLockHolder &)
{
    prepare(/* dry_run = */ false);
    BlockInputStreamPtr in = interpreter_select->execute().in;
    auto result_stream = addStreamsForLaterStages(stages, in);
    if (!updated_header)
        updated_header = std::make_unique<Block>(result_stream->getHeader());
    return result_stream;
}

const Block & MutationsInterpreter::getUpdatedHeader() const
{
    return *updated_header;
}

}
