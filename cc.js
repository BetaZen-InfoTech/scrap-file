const { MongoClient } = require( 'mongodb' );

const uri = "mongodb://dbRootUser:kgutfvTD568757GFDFHchtdYRFHf6778675CHFdcyfhCXyrfhcJcfrYXS5t75r4gJFhtcfFHdYRYIFTU45364367GJUYfJft379kgBigFdRswAawTSEey53egfytdYTFytSDtrXfDtrzAetSDKGBlG75762497@mongodb.betazeninfotech.com:27017/country_state_database_for_scrap?authSource=country_state_database_for_scrap";
const client = new MongoClient( uri );

async function batchGeocode ()
{
    try
    {
        await client.connect();
        const db = client.db( 'country_state_database_for_scrap' );
        const collection = db.collection( 'indian_state_category_scrap' );

        const result = await collection.updateMany(
            {
                status: { $ne: "entry" } // find docs where status is NOT "entry"
            },
            {
                $set: {
                    status: "entry",
                    updatedAt: new Date()
                },
                $unset: {
                    errorMessage: "",
                    scrapTotal: ""
                }
            }
        );

        console.log( `✅ Updated ${ result.modifiedCount } documents to status 'entry'` );
    } catch ( err )
    {
        console.error( '❌ MongoDB error:', err );
    } finally
    {
        await client.close();
    }
}

batchGeocode();
