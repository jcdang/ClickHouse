#include "HTTPQueryState.h"

DB::HTTPQueryState::HTTPQueryState(std::deque<std::reference_wrapper<std::ostream>> & _flush_streams):
flush_streams(_flush_streams)
{
    
}

void DB::HTTPQueryState::sendEvent(std::string & event_name, char *start, int length)
{
    std::string data = std::string(start, length);
    sendEvent(event_name, data);
}

void DB::HTTPQueryState::sendEvent(std::string & event_name, std::string & data)
{
    std::lock_guard lock(mutex);
    std::ostream & first = flush_streams.front().get();

    if (!event_name.empty())
    {
        first << "event: " << event_name << '\n';
    }
    std::string result;
    std::regex_replace(std::back_inserter(result), data.begin(), data.end(), new_line_regex, "\ndata: ");
    first << "data: " << result << "\n\n";

    for (auto & stream_ref : flush_streams) {
        stream_ref.get().flush();
    }
}

void DB::HTTPQueryState::sendEvent(const char * event_name_c_str, const char * data_c_str) {
    std::string event_name = std::string(event_name_c_str);
    std::string data = std::string(data_c_str);
    sendEvent(event_name, data);
}
