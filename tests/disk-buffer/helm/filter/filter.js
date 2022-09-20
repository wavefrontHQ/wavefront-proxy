(async () => {
    const mockttp = require('mockttp');
    const server = mockttp.getLocal({
        https: {
            keyPath: '/opt/certs/rootCA.key',
            certPath: '/opt/certs/rootCA.pem'
        }
    });
    server.forPost("/api/v2/wfproxy/config/processed").thenPassThrough();
    server.forPost("/api/v2/wfproxy/checkin").thenPassThrough();
    server.forPost("/api/v2/wfproxy/report").thenCallback(async (request) => {
        return {
            status: 500,
        };
    });

    console.log(`hi`);
    await server.start();
    console.log(`HTTPS-PROXY running on port ${server.port}`);
})();