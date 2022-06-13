(async () => {
    reports = 0;
    errors = 0;
    errorRate = Number(process.argv[2]);
    if (Number.isNaN(errorRate)) {
        errorRate = 0;
    }

    const mockttp = require('mockttp');

    const server = mockttp.getLocal({
        https: {
            keyPath: 'rootCA.key',
            certPath: 'rootCA.pem'
        }
    });

    server.forPost("/api/v2/wfproxy/checkin").thenPassThrough();

    server.forPost("/api/v2/wfproxy/report").thenCallback((request) => {
        reports++;
        resStatus = 200;
        if ((Math.random() * 100) < errorRate) {
            resStatus = 500;
            errors++;
        }
        // console.debug(`[${request.method}] -> ${request.path} res:${resStatus}`);
        return {
            status: resStatus,
        };
    });

    function stats() {
        console.log("report calls: %d - errors reported: %d (%f)", reports, errors, (errors / reports).toFixed(3));
    }

    setInterval(stats, 10000);

    await server.start();
    console.log(`Server running on port ${server.port}`);
    console.log("Point error rate %d%%", errorRate);
})();