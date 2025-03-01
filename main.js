const Queue = require('bull');
const {default: Redlock} = require("redlock");
const Redis = require("ioredis");
const redisConfig = {
    redis: {
        port: 6379, // Redis server port
        host: 'localhost', // Redis server host
    },
};

const instrumentRedLock = process.env['INSTRUMENT_REDLOCK'] || false;
if (instrumentRedLock) {
    const originalAcquire = Redlock.prototype.acquire
    Redlock.prototype.acquire = async function (resources, duration, settings) {
        console.debug(`[Redlock][${" ".repeat(32)}][${resources}] acquire called`, duration, settings);
        let lock = await originalAcquire.apply(this, [resources, duration, settings]);
        console.debug(`[Redlock][${lock.value}][${resources}] acquired`);
        return lock // Call the original release method
    }

    const originalRelease = Redlock.prototype.release;
    Redlock.prototype.release = async function (lock, settings) {
        console.debug(`[Redlock][${lock.value}][${lock.resources}] release`);
        let releaseResult = await originalRelease.apply(this, [lock, settings]);
        console.debug(`[Redlock][${lock.value}][${lock.resources}] released`, releaseResult);
        return releaseResult // Call the original release method
    }

    const originalExtend = Redlock.prototype.extend;
    Redlock.prototype.extend = async function (lock, duration, settings) {
        console.debug(`[Redlock][${lock.value}][${lock.resources}] extend`);
        let newLock = await originalExtend.apply(this, [lock, duration, settings]);
        console.debug(`[Redlock][${newLock.value}][${newLock.resources}] extended`);
        return newLock;
    }
} else {
    console.log("Redlock instrumentation disabled")
}

// Initialize Redis clients
const redis = new Redis({port: 6379, host: 'localhost'});
const redlock = new Redlock([redis]);
const queueA = new Queue('queue:a', redisConfig);

// Process jobs from the queue
queueA.process(10, (job) => {
    console.log('QA received job:', job.data.message);
    // Add your job processing logic here
    // ...
    return redlock.using(
        ['queue:a:lock'],
        1_000,
        {retryCount: -1},
        async (_signal) => {
            console.log('QA Processing job:', job.data.message);
            await new Promise(resolve => setTimeout(resolve, 5_000))
            console.log('QA Processing job done:', job.data.message);
            return {message: 'QA Job completed'}
        })
});


// Event listener for completed jobs
queueA.on('completed', (job, result) => {
    console.log(`QA Job ID ${job.data.message} completed with result:`, result);
});

// Event listener for failed jobs
queueA.on('failed', (job, err) => {
    console.error(`${new Date().toISOString()} QA Job ID ${job.data.message} failed with error:`, err);
});

const MAX_INFLIGHT = 100

async function main() {
    await queueA.obliterate({force: true});
    console.log("Waiting 2.5 seconds to start");
    await new Promise(resolve => setTimeout(resolve, 2500));
    for (let i = 0; i < MAX_INFLIGHT; i++) {
        console.log(`Queueing task ${i}`)
        await queueA.add({message: `START ${i} @ ${new Date().toISOString()}`});
        const counterA = await queueA.count()
        console.log("Stats", {
            "queued": counterA,
        });
        await new Promise(resolve => setTimeout(resolve, 2500));
    }
}

main().then(() => console.log('Main completed')).catch((err) => console.log("Main crashed", err));