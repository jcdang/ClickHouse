#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPRequestHandler.h>

namespace DB
{


class HTTPStreamHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit HTTPStreamHandler(IServer & server_);
    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    IServer & server;

    bool validate(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);

    void process(Poco::Net::HTTPServerRequest & request, std::ostream & out, std::function<void()> flush);

};

}