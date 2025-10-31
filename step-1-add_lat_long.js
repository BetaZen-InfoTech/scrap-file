const { MongoClient } = require( 'mongodb' );
const NodeGeocoder = require( 'node-geocoder' );
const cliProgress = require( 'cli-progress' );

const uri = "mongodb://dbRootUser:kgutfvTD568757GFDFHchtdYRFHf6778675CHFdcyfhCXyrfhcJcfrYXS5t75r4gJFhtcfFHdYRYIFTU45364367GJUYfJft379kgBigFdRswAawTSEey53egfytdYTFytSDtrXfDtrzAetSDKGBlG75762497@mongodb.betazeninfotech.com:27017/country_state_database_for_scrap?authSource=country_state_database_for_scrap";
const client = new MongoClient( uri );

const geocoder = NodeGeocoder( { provider: 'openstreetmap' } );

// const batchNumber = 1; // 1 = first 2000, 2 = second 2000, etc.
// const batchSize = 1000;
const BATCH_SIZE = 100; // number of documents to process per batch

async function batchGeocode ()
{
    try
    {
        await client.connect();
        const db = client.db( 'country_state_database_for_scrap' );
        const collection = db.collection( 'indian_state' );

        // Filter documents where lat or long are missing, null, or empty string
        const cursor = collection.find( {
            $or: [
                { lat: { $exists: false } },
                { lat: null },
                { lat: "" },
                { long: { $exists: false } },
                { long: null },
                { long: "" }
            ]
        } );


        // const cursor = collection.find( {
        //     $or: [
        //         { lat: { $exists: false } },
        //         { lat: null },
        //         { lat: "" },
        //         { long: { $exists: false } },
        //         { long: null },
        //         { long: "" }
        //     ]
        // } )
        //     .skip( ( batchNumber - 1 ) * batchSize )
        //     .limit( batchSize );

        const totalDocs = await cursor.count();
        console.log( `Total documents to process: ${ totalDocs }` );

        const progressBar = new cliProgress.SingleBar( {
            format: 'Processing |{bar}| {value}/{total} ({percentage}%)',
            barCompleteChar: '█',
            barIncompleteChar: '░',
            hideCursor: true
        } );
        progressBar.start( totalDocs, 0 );

        let batch = [];
        let processedCount = 0;

        while ( await cursor.hasNext() )
        {
            const doc = await cursor.next();
            processedCount++;
            progressBar.update( processedCount );

            const city = doc.City || '';
            const district = doc.District || '';
            const state = doc.State || '';
            const pincode = doc.Pincode || '';

            let query = `${ city }, ${ district }, ${ state }, ${ pincode }`;
            let status = 'Original query';

            try
            {
                let results = await geocoder.geocode( query );

                if ( results.length === 0 )
                {
                    status = 'Fallback to district + state + pincode';
                    query = `${ district }, ${ state }, ${ pincode }`;
                    results = await geocoder.geocode( query );
                }

                if ( results.length === 0 )
                {
                    status = 'Fallback to state + pincode';
                    query = `${ state }, ${ pincode }`;
                    results = await geocoder.geocode( query );
                }

                if ( results.length > 0 )
                {
                    const lat = results[ 0 ].latitude.toString();
                    const long = results[ 0 ].longitude.toString();

                    batch.push( {
                        updateOne: {
                            filter: { _id: doc._id },
                            update: { $set: { lat, long } }
                        }
                    } );

                    console.log( `✅ Query #${ processedCount }: Success | Status: ${ status } | Lat: ${ lat }, Long: ${ long }` );
                } else
                {
                    console.log( `❌ Query #${ processedCount }: No result | Last tried: ${ query }` );
                }
            } catch ( err )
            {
                console.error( `Error geocoding query #${ processedCount }: ${ query } |`, err.message );
            }

            if ( batch.length >= BATCH_SIZE )
            {
                await collection.bulkWrite( batch );
                console.log( `Processed batch of ${ batch.length } documents` );
                batch = [];
            }
        }

        if ( batch.length > 0 )
        {
            await collection.bulkWrite( batch );
            console.log( `Processed final batch of ${ batch.length } documents` );
        }

        progressBar.update( totalDocs );
        progressBar.stop();

        console.log( 'All documents processed.' );
    } catch ( err )
    {
        console.error( 'MongoDB error:', err );
    } finally
    {
        await client.close();
    }
}

batchGeocode();
