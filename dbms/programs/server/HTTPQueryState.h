#pragma once
#include <deque>
#include <ostream>
#include <regex>
namespace DB
{

class HTTPQueryState {

public:
    explicit HTTPQueryState(std::deque<std::reference_wrapper<std::ostream>> & flush_streams);

    void sendEvent(std::string & event_name, char * start, int length);
    void sendEvent(std::string & event_name, std::string  & data);
    void sendEvent(const char * event_name, const char * data);
private:
    std::regex new_line_regex = std::regex("\r?\n");
    std::deque<std::reference_wrapper<std::ostream>> & flush_streams;
};
}
