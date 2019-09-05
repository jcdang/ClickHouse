#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include "IServer.h"
#include "HTTPQueryState.h"

namespace DB
{


class HTTPStreamHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit HTTPStreamHandler(IServer & server_);
    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    IServer & server;
    Poco::Logger * log;

    bool validate(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);

    void process(Poco::Net::HTTPServerRequest & request, HTTPQueryState & query_state);

};

}
