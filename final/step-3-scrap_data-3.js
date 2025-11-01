/**
 * STEP 2 — Google Maps Place Details Scraper (Batch Version)
 * -----------------------------------------------------------
 * ✅ Loads all place URLs from 'indian_state_category_scrap_urls'
 * ✅ Scrapes details in batches (parallel)
 * ✅ Updates each record in MongoDB
 * ✅ Removes MongoDB cursor usage for simplicity
 * ✅ Auto-restarts on any unexpected error
 */

const mongoose = require( "mongoose" );
const { chromium } = require( "playwright" );
const cliProgress = require( "cli-progress" );
const ScrapUrl = require( "../models/ScrapUrl" );

// ------------------ CONFIG ------------------
const MONGO_URI =
    "mongodb://dbRootUser:kgutfvTD568757GFDFHchtdYRFHf6778675CHFdcyfhCXyrfhcJcfrYXS5t75r4gJFhtcfFHdYRYIFTU45364367GJUYfJft379kgBigFdRswAawTSEey53egfytdYTFytSDtrXfDtrzAetSDKGBlG75762497@mongodb.betazeninfotech.com:27017/country_state_database_for_scrap?authSource=country_state_database_for_scrap";

const DB_NAME = "country_state_database_for_scrap";
const BATCH_ID = 3;
const BATCH_SIZE = 15000;

const SCRAP_BATCH_SIZE = 15; // number of URLs to scrape in parallel
const MAX_LIMIT = 200; // number of documents to load at once
const RESTART_DELAY = 5000; // ms before restart if crash occurs



// ------------------ MAIN SCRAPER ------------------
async function scrapePlaceDetails ()
{

    // ------------------ MONGODB CONNECTION ------------------
    await mongoose.connect( MONGO_URI, { dbName: DB_NAME } );
    console.log( "✅ Connected to MongoDB" );

    const skipCount = ( BATCH_ID - 1 ) * BATCH_SIZE;

    const pendingDocs = await ScrapUrl.find( {
        $or: [ { status: { $exists: false } }, { status: { $ne: "completed" } } ],
        url: { $exists: true, $ne: "" },
    } )
        .skip( skipCount )
        .limit( BATCH_SIZE );

    console.log( `📋 Total pending URLs loaded: ${ pendingDocs.length }` );
    if ( !pendingDocs.length )
    {
        console.log( "✅ No pending documents found. Exiting..." );
        await mongoose.disconnect();
        return;
    }

    const progressBar = new cliProgress.SingleBar(
        {
            format: "Scraping |{bar}| {value}/{total} ({percentage}%)",
            barCompleteChar: "█",
            barIncompleteChar: "░",
            hideCursor: true,
        },
        cliProgress.Presets.shades_classic
    );
    progressBar.start( pendingDocs.length, 0 );

    const browser = await chromium.launch( { headless: true } );
    const context = await browser.newContext();

    for ( let i = 0; i < pendingDocs.length; i += SCRAP_BATCH_SIZE )
    {
        const batch = pendingDocs.slice( i, i + SCRAP_BATCH_SIZE );
        await processBatch( batch, context, progressBar );
    }

    progressBar.stop();
    await browser.close();
    await mongoose.disconnect();
    console.log( "🎯 All place details scraped successfully." );
}

// ------------------ BATCH PROCESSOR ------------------
async function processBatch ( batch, context, progressBar )
{
    const tasks = batch.map( async ( doc ) =>
    {
        const page = await context.newPage();
        let data = {};

        try
        {
            await page.goto( doc.url, { waitUntil: "domcontentloaded", timeout: 120000 } );
            await page.waitForTimeout( 4000 );

            data.status = "completed";
            data.updatedAt = new Date();
            data.scrapedAt = new Date();

            // --- Name (EN)
            try
            {
                const el = await page.$( "h1.DUwDvf" );
                data.name_en = el ? ( await el.textContent() ).trim() : null;
            } catch { }

            // --- Local Name
            try
            {
                const el = await page.$( "h2.bwoZTb span" );
                data.name_local = el ? ( await el.textContent() ).trim() : null;
            } catch { }

            // --- Category
            try
            {
                const el = await page.$( 'button[jsaction="pane.wfvdle18.category"]' );
                data.category = el ? ( await el.textContent() ).trim() : null;
            } catch { }

            // --- Rating
            try
            {
                const el = await page.$( 'div.F7nice span[aria-hidden="true"]' );
                data.rating = el ? ( await el.textContent() ).replace( /[(),]/g, "" ).trim() : null;
            } catch { }

            // --- Reviews Count
            try
            {
                const el = await page.$( 'div.F7nice span[aria-label*="review"]' );
                data.reviews_count = el
                    ? ( await el.textContent() ).replace( /[(),\s]/g, "" ).trim()
                    : null;
            } catch { }

            // --- Price Range
            try
            {
                const el = await page.$( 'div[aria-label*="per person"]' );
                data.price_range = el ? ( await el.textContent() ).trim() : null;
            } catch { }

            // --- Phone
            try
            {
                const el = await page.$( 'button[data-item-id^="phone"] .Io6YTe' );
                let phone = el ? ( await el.textContent() ).trim() : null;
                if ( phone ) phone = phone.replace( /[^0-9+]/g, "" ).trim();
                data.phone = phone || null;
            } catch { }

            // --- Website
            try
            {
                const el = await page.$( 'a[data-item-id="authority"] .Io6YTe' );
                data.website = el ? ( await el.textContent() ).trim() : null;
            } catch { }

            // --- Address
            try
            {
                const el = await page.$( 'button[data-item-id="address"] .Io6YTe' );
                data.address = el ? ( await el.textContent() ).trim() : null;
            } catch { }

            // --- Plus Code
            try
            {
                const el = await page.$( 'button[data-item-id="oloc"] .Io6YTe' );
                data.plus_code = el ? ( await el.textContent() ).trim() : null;
            } catch { }

            // --- Price per Person
            try
            {
                const el = await page.$( "div.MNVeJb .BfVpR" );
                data.price_per_person = el ? ( await el.textContent() ).trim() : null;
            } catch { }

            if ( !data.name_en )
            {
                throw new Error( "Place name not found, possible page load issue." );
            } else
            {
                console.log( `✅ Scraped: ${ data.name_en }` );
                await ScrapUrl.updateOne( { _id: doc._id }, { $set: data } );
            }

            console.log( `✅ Updated: ${ doc._id }` );
        } catch ( err )
        {
            console.log( `❌ Failed ${ doc._id }: ${ err.message }` );
            await ScrapUrl.updateOne(
                { _id: doc._id },
                { $set: { status: "error", updatedAt: new Date() } }
            );
        } finally
        {
            await page.close();
            progressBar.increment();
        }
    } );

    await Promise.all( tasks );
}

// ------------------ AUTO-RESTART WRAPPER ------------------
async function safeRun ()
{
    let attempt = 1;
    while ( true )
    {
        try
        {
            console.log( `\n🌀 Starting scraper (Attempt #${ attempt })` );
            await scrapePlaceDetails();
            console.log( "✅ All batches completed successfully." );
            // break; // Exit loop if successful
        } catch ( err )
        {
            console.error( `💥 Error in main scraper: ${ err.message }` );
            console.log( `🔁 Restarting in ${ RESTART_DELAY / 1000 } seconds...` );
            attempt++;
            await new Promise( ( r ) => setTimeout( r, RESTART_DELAY ) );
        }
    }
}

// ------------------ RUN ------------------
safeRun();

// Graceful exit handler
process.on( "SIGINT", async () =>
{
    console.log( "\n🛑 Received SIGINT. Closing MongoDB..." );
    await mongoose.disconnect();
    process.exit( 0 );
} );
