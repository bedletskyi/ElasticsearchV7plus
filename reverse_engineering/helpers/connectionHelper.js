const elasticsearch = require('elasticsearch');
const fs = require('fs');

let _client = null;

const connect = (connectionInfo) => {
    let authString = "";
    let connectionParams = {};

    if (_client !== null) {
        return { _client };
    }

    if (connectionInfo.username) {
        authString = connectionInfo.username;
    }

    if (connectionInfo.password) {
        authString += ':' + connectionInfo.password;
    }

    if (connectionInfo.connectionType === 'Direct connection') {
        connectionParams.host = {
            protocol: connectionInfo.protocol,
            host: connectionInfo.host,
            port: connectionInfo.port,
            path: connectionInfo.path,
            auth: authString
        };
    } else if (connectionInfo.connectionType === 'Replica set or Sharded cluster') {
        connectionParams.hosts = connectionInfo.hosts.map(socket => {
            return {
                host: socket.host,
                port: socket.port,
                protocol: connectionInfo.protocol,
                auth: authString
            };
        });
    } else {
        throw new Error('Invalid connection parameters');
    }

    if (connectionInfo.is_ssl) {
        connectionParams.ssl = {
            ca: fs.readFileSync(connectionInfo.ca),
            rejectUnauthorized: connectionInfo.rejectUnauthorized
        };
    }

    _client = new elasticsearch.Client(connectionParams);
    return { _client, connectionParams };
};

const disconnect = () => {
    if (_client) {
        _client.close();
        _client = null;
    }
    connectionParams = {};
};

module.exports = {
    connect,
    disconnect
}
