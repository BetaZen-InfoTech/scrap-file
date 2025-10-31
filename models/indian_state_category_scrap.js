/**
 * IndianStateCategoryScrap Schema
 * ---------------------------------------
 * Stores input records for Google Maps scraping
 * Each record represents one (state + category + location) search query
 */

const mongoose = require( 'mongoose' );

const IndianStateCategoryScrapSchema = new mongoose.Schema(
    {
        stateId: {
            type: mongoose.Schema.Types.ObjectId,
            required: true,
        },
        categoryId: {
            type: mongoose.Schema.Types.ObjectId,
            required: true,
        },
        searchString: {
            type: String,
            required: true,
            trim: true,
        },
        encodedQuery: {
            type: String,
            required: true,
            trim: true,
        },
        CategoryName: {
            type: String,
            trim: true,
        },
        CategoryType: {
            type: String,
            trim: true,
        },
        District: {
            type: String,
            trim: true,
        },
        Pincode: {
            type: String,
            trim: true,
        },
        State: {
            type: String,
            trim: true,
        },
        lat: {
            type: String, // or Number if you plan numeric operations
            trim: true,
        },
        long: {
            type: String, // or Number
            trim: true,
        },
        status: {
            type: String,
            enum: [ 'entry', 'pending', 'completed', 'failed' ],
            default: 'entry',
        },
        scrapTotal: {
            type: Number,
            default: 0,
        },
        errorMessage: {
            type: String,
        },
        createdAt: {
            type: Date,
            default: Date.now,
        },
        updatedAt: {
            type: Date,
            default: Date.now,
        },
    },
    {
        collection: 'indian_state_category_scrap',
        timestamps: true,
    }
);

// Optional: add indexes for faster querying
IndianStateCategoryScrapSchema.index( { stateId: 1 } );
IndianStateCategoryScrapSchema.index( { categoryId: 1 } );
IndianStateCategoryScrapSchema.index( { status: 1 } );
IndianStateCategoryScrapSchema.index( { Pincode: 1 } );

module.exports = mongoose.model(
    'IndianStateCategoryScrap',
    IndianStateCategoryScrapSchema
);
