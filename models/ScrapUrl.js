/**
 * ScrapUrl Schema
 * ---------------------
 * Stores all scraped Google Maps URLs
 * and keeps references to state/category/source query.
 */

const mongoose = require( 'mongoose' );

const ScrapUrlSchema = new mongoose.Schema(
    {
        // Core scraped data
        url: { type: String, required: true, unique: true },
        scrapParentId: { type: mongoose.Schema.Types.ObjectId, ref: 'indian_state_category_scrap' },

        // Reference info from parent
        stateId: { type: mongoose.Schema.Types.ObjectId },
        categoryId: { type: mongoose.Schema.Types.ObjectId },
        State: { type: String },
        District: { type: String },
        Pincode: { type: String },
        CategoryName: { type: String },
        CategoryType: { type: String },

        // Original query context
        encodedQuery: { type: String },
        searchString: { type: String },

        // Location info
        lat: { type: String },
        long: { type: String },

        // Scrap Data
        status: { type: String },
        scrapedAt: { type: Date },
        name_en: { type: String },
        name_local: { type: String },
        category: { type: String },
        rating: { type: String },
        reviews_count: { type: String },
        price_range: { type: String },
        phone: { type: String },
        website: { type: String },
        address: { type: String },
        plus_code: { type: String },
        price_per_person: { type: String },

        // Metadata
        createdAt: { type: Date, default: Date.now },
        updatedAt: { type: Date },
    },
    { collection: 'indian_state_category_scrap_urls' }
);

// Helpful indexes (not duplicates)
ScrapUrlSchema.index( { stateId: 1 } );
ScrapUrlSchema.index( { categoryId: 1 } );
ScrapUrlSchema.index( { scrapParentId: 1 } );
ScrapUrlSchema.index( { State: 1 } );
ScrapUrlSchema.index( { District: 1 } );
ScrapUrlSchema.index( { Pincode: 1 } );
ScrapUrlSchema.index( { CategoryName: 1 } );
ScrapUrlSchema.index( { CategoryType: 1 } );
ScrapUrlSchema.index( { category: 1 } );

module.exports = mongoose.model( 'ScrapUrl', ScrapUrlSchema );
