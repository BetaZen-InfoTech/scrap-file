/**
 * Google Maps Scraper with MongoDB + Playwright
 * -------------------------------------------------
 * ✅ Reads only pending records (status != "completed")
 * ✅ Scrapes Google Maps "place" URLs
 * ✅ Inserts URLs in bulk (prevents duplicates)
 * ✅ Updates input collection: { status, scrapTotal, updatedAt }
 * ✅ Auto-restarts if crash occurs
 * ✅ All batches run in parallel
 * ✅ Enhanced with robust try/catch and structured logging
 * ✅ Now includes CPU usage monitoring and throttling
 */

const mongoose = require( 'mongoose' );
const { chromium } = require( 'playwright' );
const cliProgress = require( 'cli-progress' );
const pidusage = require( 'pidusage' );
const os = require( 'os' );
const ScrapUrl = require( '../models/ScrapUrl' );
const IndianStateCategoryScrap = require( '../models/indian_state_category_scrap' );

// ------------------ MONGODB CONFIG ------------------
const uri =
    'mongodb://dbRootUser:kgutfvTD568757GFDFHchtdYRFHf6778675CHFdcyfhCXyrfhcJcfrYXS5t75r4gJFhtcfFHdYRYIFTU45364367GJUYfJft379kgBigFdRswAawTSEey53egfytdYTFytSDtrXfDtrzAetSDKGBlG75762497@mongodb.betazeninfotech.com:27017/country_state_database_for_scrap?authSource=country_state_database_for_scrap';

const DB_NAME = 'country_state_database_for_scrap';
const BATCH_ID = 6; // Batch group identifier
const BATCH_SIZE = 20000; // Total records per batch
const SCRAP_BATCH_SIZE = 3; // Parallel scraping per group

let connection;
let ScrapUrlModel;
let IndianModel;
let browser;


// ------------------ CPU MONITOR ------------------
async function checkCPUUsage ( maxCPU = 85 )
{
    try
    {
        const stats = await pidusage( process.pid );
        const cpu = stats.cpu;
        if ( cpu > maxCPU )
        {
            console.log( `⚠️  High CPU usage detected: ${ cpu.toFixed( 1 ) }%. Pausing 30s to cool down...` );
            await new Promise( res => setTimeout( res, 30000 ) ); // pause 30 seconds
        }
    } catch ( err )
    {
        console.log( "⚠️ CPU check failed:", err.message );
    }
}

// ------------------ PROCESS INDIVIDUAL PLACE ------------------
async function processBatch ( doc, context, idx )
{
    const page = await context.newPage();
    console.log( `\n⚙️ [processBatch] Processing index: ${ idx }, URL: ${ doc.url }` );

    try
    {
        console.log( `🌐 [processBatch] Navigating to: ${ doc.url }` );
        await page.goto( doc.url, { waitUntil: 'domcontentloaded', timeout: 30000 } );
        await page.waitForTimeout( 4000 );

        doc.updatedAt = new Date();

        try
        {
            const el = await page.$( 'h1.DUwDvf' );
            doc.name_en = el ? ( await el.textContent() ).trim() : null;
        } catch ( err )
        {
            console.log( `⚠️ [processBatch] Name (EN) not found: ${ err.message }` );
        }

        try
        {
            const el = await page.$( 'h2.bwoZTb span' );
            doc.name_local = el ? ( await el.textContent() ).trim() : null;
        } catch
        {
            console.log( '⚠️ [processBatch] Local name missing' );
        }

        try
        {
            const el = await page.$( 'button[jsaction="pane.wfvdle18.category"]' );
            doc.category = el ? ( await el.textContent() ).trim() : null;
        } catch
        {
            console.log( '⚠️ [processBatch] Category not found' );
        }

        try
        {
            const el = await page.$( 'div.F7nice span[aria-hidden="true"]' );
            doc.rating = el ? ( await el.textContent() ).replace( /[(),]/g, '' ).trim() : null;
        } catch
        {
            console.log( '⚠️ [processBatch] Rating not found' );
        }

        try
        {
            const el = await page.$( 'div.F7nice span[aria-label*="review"]' );
            doc.reviews_count = el ? ( await el.textContent() ).replace( /[(),\s]/g, '' ).trim() : null;
        } catch
        {
            console.log( '⚠️ [processBatch] Review count not found' );
        }

        try
        {
            const el = await page.$( 'div[aria-label*="per person"]' );
            doc.price_range = el ? ( await el.textContent() ).trim() : null;
        } catch { }

        try
        {
            const el = await page.$( 'button[data-item-id^="phone"] .Io6YTe' );
            let phone = el ? ( await el.textContent() ).trim() : null;
            if ( phone ) phone = phone.replace( /[^0-9+]/g, '' ).trim();
            doc.phone = phone || null;
        } catch
        {
            console.log( '⚠️ [processBatch] Phone not found' );
        }

        try
        {
            const el = await page.$( 'a[data-item-id="authority"] .Io6YTe' );
            doc.website = el ? ( await el.textContent() ).trim() : null;
        } catch { }

        try
        {
            const el = await page.$( 'button[data-item-id="address"] .Io6YTe' );
            doc.address = el ? ( await el.textContent() ).trim() : null;
        } catch { }

        try
        {
            const el = await page.$( 'button[data-item-id="oloc"] .Io6YTe' );
            doc.plus_code = el ? ( await el.textContent() ).trim() : null;
        } catch { }

        try
        {
            const el = await page.$( 'div.MNVeJb .BfVpR' );
            doc.price_per_person = el ? ( await el.textContent() ).trim() : null;
        } catch { }

        if ( !doc.name_en )
        {
            console.log( '⚠️ [processBatch] Place name not found, possible page load issue.' );
        } else
        {
            doc.status = 'completed';
            doc.scrapedAt = new Date();
            console.log( `✅ [processBatch] Scraped successfully: ${ doc.name_en }` );
        }

        try
        {
            const result = await ScrapUrlModel.insertOne( doc );
            console.log( `✅ [processBatch] Inserted document: ${ result.insertedId || 'OK' }` );
        } catch ( err )
        {
            console.error( `❌ [processBatch] Mongo insert failed: ${ err.message }` );
        }
    } catch ( err )
    {
        console.error( `❌ [processBatch] General failure for ${ doc.url }: ${ err.message }` );
        try
        {
            doc.status = 'error';
            await ScrapUrlModel.insertOne( doc );
        } catch ( updateErr )
        {
            console.error( `⚠️ [processBatch] Failed to mark error status: ${ updateErr.message }` );
        }
    } finally
    {
        await page.close();
        console.log( `🧹 [processBatch] Closed page for ${ doc.url }` );
    }
}

// ------------------ MAIN BATCH FUNCTION ------------------
async function batchGeocode ( BATCH_ID )
{
    console.log( `\n🚀 Starting batch ${ BATCH_ID }...` );

    try
    {
        const col1 = await ScrapUrlModel.countDocuments( {} );
        const col2 = await IndianModel.countDocuments( {} );
        console.log( `[batchGeocode] ScrapUrl: ${ col1 } | IndianModel: ${ col2 }` );

        const from = ( BATCH_ID - 1 ) * BATCH_SIZE;
        const to = from + BATCH_SIZE;

        const query = {
            $or: [ { status: { $exists: false } }, { status: { $ne: 'completed' } } ],
        };

        const totalDocs = await IndianModel.countDocuments( query );
        console.log( `📋 [Batch ${ BATCH_ID }] Pending documents: ${ totalDocs }` );

        const docs = await IndianModel.find( query ).skip( from ).limit( BATCH_SIZE ).lean();
        console.log( `📦 [Batch ${ BATCH_ID }] Processing records ${ from + 1 }–${ to }` );

        const progressBar = new cliProgress.SingleBar(
            {
                format: `Batch ${ BATCH_ID } |{bar}| {value}/{total} ({percentage}%)`,
                barCompleteChar: '█',
                barIncompleteChar: '░',
                hideCursor: true,
            },
            cliProgress.Presets.shades_classic
        );

        progressBar.start( docs.length, 0 );

        for ( let i = 0; i < docs.length; i++ )
        {
            const doc = docs[ i ];
            progressBar.update( i + 1 );

            try
            {
                const { _id: id, encodedQuery, searchString, lat, long } = doc;
                if ( !encodedQuery )
                {
                    console.log( `⚠️ [Batch ${ BATCH_ID }] Skipping ID ${ id } - missing encodedQuery` );
                    continue;
                }

                const googleUrl = `https://www.google.com/maps/search/${ encodedQuery }/@${ lat },${ long }`;
                console.log( `\n🗺️ [${ BATCH_ID }:${ i + 1 }/${ docs.length }] ${ searchString } (${ id })` );
                console.time( `⏱️ Batch ${ BATCH_ID } Record ${ i + 1 }` );

                // 🧠 Check CPU before launching browser
                await checkCPUUsage();

                const context = await browser.newContext();

                try
                {
                    const page = await context.newPage();
                    await page.goto( googleUrl, { waitUntil: 'domcontentloaded', timeout: 120000 } );
                    await page.waitForTimeout( 5000 );
                    await page.waitForSelector( 'div[role="main"]', { timeout: 60000 } );

                    const scrollable = await page.$( 'div[role="feed"]' );
                    if ( !scrollable ) throw new Error( 'Scrollable container not found' );

                    console.log( `📜 [Batch ${ BATCH_ID }] Scrolling results...` );
                    let lastHeight = 0, sameCount = 0;
                    while ( sameCount < 3 )
                    {
                        const currentHeight = await scrollable.evaluate( ( n ) => n.scrollHeight );
                        await scrollable.evaluate( ( n ) => n.scrollBy( 0, n.scrollHeight ) );
                        await page.waitForTimeout( 2000 );
                        const newHeight = await scrollable.evaluate( ( n ) => n.scrollHeight );
                        if ( newHeight === lastHeight ) sameCount++;
                        else sameCount = 0;
                        lastHeight = newHeight;
                    }

                    const urls = await page.$$eval(
                        'a[href^="https://www.google.com/maps/place/"]',
                        ( links ) => [ ...new Set( links.map( ( link ) => link.href.trim() ) ) ]
                    );

                    let totalScraped = urls.length;
                    console.log( `✅ [Batch ${ BATCH_ID }] Found ${ totalScraped } URLs` );

                    if ( totalScraped > 0 )
                    {
                        const urlDocs = urls.map( ( url ) => ( {
                            url,
                            scrapParentId: id,
                            stateId: doc.stateId,
                            categoryId: doc.categoryId,
                            State: doc.State,
                            District: doc.District,
                            Pincode: doc.Pincode,
                            CategoryName: doc.CategoryName,
                            CategoryType: doc.CategoryType,
                            encodedQuery,
                            searchString,
                            lat,
                            long,
                            createdAt: new Date(),
                            updatedAt: new Date(),
                            status: 'waiting',
                        } ) );

                        for ( let j = 0; j < urlDocs.length; j += SCRAP_BATCH_SIZE )
                        {
                            await checkCPUUsage(); // 🧠 check before each small batch
                            const batchDocs = urlDocs.slice( j, j + SCRAP_BATCH_SIZE );
                            await Promise.all(
                                batchDocs.map( ( d, idx ) => processBatch( d, context, j + idx ) )
                            );
                        }

                        try
                        {
                            await ScrapUrlModel.insertMany( urlDocs, { ordered: false } );
                            console.log( `🟢 [Batch ${ BATCH_ID }] Inserted ${ urlDocs.length } URLs` );
                        } catch ( err )
                        {
                            if ( err.writeErrors )
                            {
                                const dupCount = err.writeErrors.filter( ( e ) => e.code === 11000 ).length;
                                console.log( `⚠️ [Batch ${ BATCH_ID }] ${ dupCount } duplicate URLs skipped` );
                                totalScraped -= dupCount;
                            } else
                            {
                                console.error( `❌ [Batch ${ BATCH_ID }] Insert error:`, err.message );
                            }
                        }

                        await IndianModel.updateOne(
                            { _id: id },
                            { $inc: { scrapTotal: totalScraped } }
                        );

                        console.log( `📊 [Batch ${ BATCH_ID }] Record updated (${ totalScraped } URLs)` );
                    } else
                    {
                        console.log( `⚠️ [Batch ${ BATCH_ID }] No URLs found to insert` );
                    }
                } catch ( innerErr )
                {
                    console.error( `❌ [Batch ${ BATCH_ID }] Error ID ${ doc._id }: ${ innerErr.message }` );
                    await IndianModel.updateOne(
                        { _id: doc._id },
                        {
                            $set: {
                                status: 'failed',
                                errorMessage: innerErr.message,
                                updatedAt: new Date(),
                            },
                        }
                    );
                } finally
                {
                    await context.close();
                    console.timeEnd( `⏱️ Batch ${ BATCH_ID } Record ${ i + 1 }` );
                }
            } catch ( loopErr )
            {
                console.log( `⚠️ [Batch ${ BATCH_ID }] Loop error: ${ loopErr.message }` );
            }
        }

        progressBar.stop();
        console.log( `🎯 [Batch ${ BATCH_ID }] Completed successfully.` );
    } catch ( err )
    {
        console.error( `❌ [batchGeocode] Fatal error in batch ${ BATCH_ID }: ${ err.message }` );
    } finally
    {
        console.log( `🔒 [batchGeocode] MongoDB connection closed for batch ${ BATCH_ID }` );
    }
}

// ------------------ AUTO-RESTART WRAPPER ------------------
async function safeRun ()
{

    let attempt = 1;
    const startBatch = ( BATCH_ID - 1 ) * 4 + 1;
    const endBatch = ( BATCH_ID - 1 ) * 4 + 4;

    browser = await chromium.launch( { headless: true } );

    try
    {
        console.log( '🧩 Connecting to MongoDB...' );
        connection = await mongoose.createConnection( uri, {
            dbName: DB_NAME,
            serverSelectionTimeoutMS: 20000,
        } );

        ScrapUrlModel = connection.model( 'ScrapUrl', ScrapUrl.schema );
        IndianModel = connection.model( 'IndianStateCategoryScrap', IndianStateCategoryScrap.schema );

        while ( true )
        {
            try
            {
                console.log( `\n🌀 Starting main scraper (Attempt #${ attempt })` );
                console.log( `📦 Running batches ${ startBatch } → ${ endBatch } in parallel...` );

                await Promise.all(
                    Array.from( { length: endBatch - startBatch + 1 }, ( _, i ) =>
                        batchGeocode( startBatch + i )
                    )
                );

                console.log( '✅ All batches completed successfully.' );
                break;
            } catch ( err )
            {
                console.error( `🔥 Crash detected (Attempt #${ attempt }): ${ err.message }` );
                attempt++;
                console.log( '🔁 Restarting scraper in 10 seconds...' );
                await new Promise( ( r ) => setTimeout( r, 10000 ) );
            }
        }
    } catch ( connErr )
    {
        console.error( `❌ [safeRun] Mongo connection error: ${ connErr.message }` );
    }
}

// ------------------ BACKGROUND CPU WATCHER ------------------
setInterval( async () =>
{
    try
    {
        const stats = await pidusage( process.pid );
        if ( stats.cpu > 90 )
        {
            console.log( `🚨 Sustained CPU load (${ stats.cpu.toFixed( 1 ) }%). Restarting process...` );
            process.exit( 1 ); // PM2 or wrapper restarts scraper
        }
    } catch ( err )
    {
        console.log( "⚠️ Background CPU monitor error:", err.message );
    }
}, 15000 ); // every 15 seconds

// ------------------ GLOBAL CRASH HANDLERS ------------------
process.on( 'uncaughtException', ( err ) =>
{
    console.error( '🚨 Uncaught Exception:', err );
    console.log( '🔁 Restarting in 10 seconds...' );
    setTimeout( () => safeRun(), 10000 );
} );

process.on( 'unhandledRejection', ( err ) =>
{
    console.error( '🚨 Unhandled Rejection:', err );
    console.log( '🔁 Restarting in 10 seconds...' );
    setTimeout( () => safeRun(), 10000 );
} );

// ------------------ START SCRIPT ------------------
safeRun();
