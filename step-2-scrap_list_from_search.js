/**
 * Google Maps Scraper with MongoDB + Playwright
 * -------------------------------------------------
 * ✅ Reads only pending records (status != "completed")
 * ✅ Scrapes Google Maps "place" URLs
 * ✅ Inserts URLs in bulk (prevents duplicates)
 * ✅ Updates input collection: { status, scrapTotal, updatedAt }
 * ✅ Auto-restarts if crash occurs
 * ✅ All batches run in parallel
 */

const mongoose = require( 'mongoose' );
const { chromium } = require( 'playwright' );
const cliProgress = require( 'cli-progress' );
const ScrapUrl = require( './models/ScrapUrl' );
const IndianStateCategoryScrap = require( './models/indian_state_category_scrap' );

// ------------------ MONGODB CONFIG ------------------
const uri =
    'mongodb://dbRootUser:kgutfvTD568757GFDFHchtdYRFHf6778675CHFdcyfhCXyrfhcJcfrYXS5t75r4gJFhtcfFHdYRYIFTU45364367GJUYfJft379kgBigFdRswAawTSEey53egfytdYTFytSDtrXfDtrzAetSDKGBlG75762497@mongodb.betazeninfotech.com:27017/country_state_database_for_scrap?authSource=country_state_database_for_scrap';

const DB_NAME = 'country_state_database_for_scrap';
const BATCH_SIZE = 100000;

// ------------------ BATCH FUNCTION ------------------
async function batchGeocode ( BATCH_ID )
{
    console.log( `\n🚀 Starting batch ${ BATCH_ID }...` );

    const connection = await mongoose.createConnection( uri, {
        dbName: DB_NAME,
        serverSelectionTimeoutMS: 20000,
    } );

    const ScrapUrlModel = connection.model( 'ScrapUrl', ScrapUrl.schema );
    const IndianModel = connection.model( 'IndianStateCategoryScrap', IndianStateCategoryScrap.schema );

    const from = ( BATCH_ID - 1 ) * BATCH_SIZE;
    const to = from + BATCH_SIZE;

    const query = {
        $or: [ { status: { $exists: false } }, { status: { $ne: 'completed' } } ],
    };

    const totalDocs = await IndianModel.countDocuments( query );
    console.log( `📋 Batch ${ BATCH_ID }: Total pending documents: ${ totalDocs }` );

    const docs = await IndianModel.find( query ).skip( from ).limit( BATCH_SIZE ).lean();
    console.log( `📦 Batch ${ BATCH_ID } — Processing records ${ from + 1 }–${ to }` );

    const progressBar = new cliProgress.SingleBar( {
        format: `Batch ${ BATCH_ID } |{bar}| {value}/{total} ({percentage}%)`,
        barCompleteChar: '█',
        barIncompleteChar: '░',
        hideCursor: true,
    } );

    progressBar.start( docs.length, 0 );

    for ( let i = 0; i < docs.length; i++ )
    {
        const doc = docs[ i ];
        progressBar.update( i + 1 );

        const { _id: id, encodedQuery, searchString, lat, long } = doc;
        if ( !encodedQuery )
        {
            console.log( `⚠️ [Batch ${ BATCH_ID }] Skipping ID ${ id } - missing encodedQuery` );
            continue;
        }

        // const googleUrl = `https://www.google.com/maps/search/${ encodedQuery }/@${ lat },${ long },10z`;
        const googleUrl = `https://www.google.com/maps/search/${ encodedQuery }/@${ lat },${ long }`;
        console.log( `\n🗺️ [${ BATCH_ID }:${ i + 1 }/${ docs.length }] ${ searchString } (${ id })` );
        console.time( `⏱️ Batch ${ BATCH_ID } Record ${ i + 1 }` );

        const browser = await chromium.launch( { headless: true } );
        const page = await browser.newPage();
        let totalScraped = 0;

        try
        {
            await page.goto( googleUrl, { waitUntil: 'domcontentloaded', timeout: 120000 } );
            await page.waitForTimeout( 5000 );
            await page.waitForSelector( 'div[role="main"]', { timeout: 60000 } );

            const scrollable = await page.$( 'div[role="feed"]' );
            if ( !scrollable ) throw new Error( 'Scrollable container not found' );

            console.log( `📜 [Batch ${ BATCH_ID }] Scrolling results...` );
            let lastHeight = 0,
                sameCount = 0;
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

            totalScraped = urls.length;
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
                } ) );

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
                    { $set: { status: 'completed', updatedAt: new Date() }, $inc: { scrapTotal: totalScraped } }
                );
                console.log( `📊 [Batch ${ BATCH_ID }] Record marked completed (${ totalScraped } URLs)` );
            } else
            {
                console.log( `⚠️ [Batch ${ BATCH_ID }] No URLs found to insert` );
            }
        } catch ( err )
        {
            console.error( `❌ [Batch ${ BATCH_ID }] Error ID ${ id }:`, err.message );
            await IndianModel.updateOne(
                { _id: id },
                { $set: { status: 'failed', errorMessage: err.message, updatedAt: new Date() } }
            );
        } finally
        {
            await browser.close();
            console.timeEnd( `⏱️ Batch ${ BATCH_ID } Record ${ i + 1 }` );
        }
    }

    progressBar.stop();
    console.log( `🎯 Batch ${ BATCH_ID } completed successfully.` );
    await connection.close();
}

// ------------------ AUTO-RESTART WRAPPER ------------------
async function safeRun ()
{
    let attempt = 1;
    while ( true )
    {
        try
        {
            console.log( `\n🌀 Starting main scraper (Attempt #${ attempt })` );
            const batchCount = 6;

            // ✅ Run all batches in parallel
            await Promise.all( Array.from( { length: batchCount }, ( _, i ) => batchGeocode( i + 1 ) ) );

            console.log( '✅ All batches completed successfully.' );
            // break;       // Exit loop if successful
        } catch ( err )
        {
            console.error( `🔥 Crash detected (Attempt #${ attempt }):`, err.message );
            attempt++;
            console.log( '🔁 Restarting scraper in 10 seconds...' );
            await new Promise( ( r ) => setTimeout( r, 10000 ) );
        }
    }
}

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

// ------------------ RUN ------------------
safeRun();
