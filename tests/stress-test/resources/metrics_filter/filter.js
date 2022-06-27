errorRate = Number(process.argv[2]);
if (Number.isNaN(errorRate)) {
    errorRate = 0;
}

delay = Number(process.argv[3]);
if (Number.isNaN(delay)) {
    delay = 0;
}

(async () => {
    reports = 0;
    errors = 0;

    const mockttp = require('mockttp');

    const server = mockttp.getLocal({
        https: {
            keyPath: '../certs/rootCA.key',
            certPath: '../certs/rootCA.pem'
        }
    });

    // server.forAnyRequest().thenCallback(async (request) => {
    //     console.log('reques: ', request);
    // });


    server.forPost("/api/v2/wfproxy/config/processed").thenPassThrough();

    server.forPost("/api/v2/wfproxy/checkin").thenPassThrough();

    server.forPost("/api/v2/wfproxy/report").thenCallback(async (request) => {
        reports++;
        resStatus = 200;
        if ((Math.random() * 100) < errorRate) {
            resStatus = 500;
            errors++;
        }
        await sleep(delay * 1000)
        return {
            status: resStatus,
        };
    });

    function stats() {
        console.log("report calls: %d - errors reported: %d (%f)", reports, errors, (errors / reports).toFixed(3));
    }

    setInterval(stats, 10000);

    await server.start();
    console.log(`HTTPS-PROXY running on port ${server.port}`);
    console.log("Point error rate %d%%", errorRate);
})();

function sleep(millis) {
    return new Promise(resolve => setTimeout(resolve, millis));
}


console.log("hi");
const express = require('express');
http = require('http');

const app = express();

const server = app.listen(7000, () => {
    console.log(`Admin UI running on PORT ${server.address().port}`);
});

var bodyParser = require('body-parser')
app.use(bodyParser.urlencoded({
    extended: true
}));

app.post('/error_rate', (req, res) => {
    errorRate = req.body.val
    console.log("error_rate --> " + req.body.val)
    res.send('ok');
})

app.post('/delay', (req, res) => {
    delay = req.body.val
    console.log("delay --> " + req.body.val)
    res.send('ok');
})

app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});


app._router.stack.forEach(function (r) {
    if (r.route && r.route.path) {
        console.log(r.route.path)
    }
})