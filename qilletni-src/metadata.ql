import "postgres:postgres.ql"

/**
 * An entity to handle metadata on meta-allowable objects. Only one should be created per application context.
 * Objects that allow metadata on them are:
 * songs, albums, artists, collections.
 * 
 * Metadata attributes may be owned by a single object, or inherited from parent objects. The hierarchy of inheritance
 * is:
 * song -> album -> artist
 * Such that songs can get metadata from albums and then artists, and albums can get metadata from artists.
 */
entity Metadata {
    
    Connection _connection
    
    /**
     * Create a new metadata object with the given database connection.
     * For depending libraries, use the `createMetadata` function instead of the constructor.
     *
     * @param _connection An active connection to a database
     */
    Metadata(_connection)
    
    static fun createMetadata(database) {
        Connection connection = database.createConnection()
        if (connection.isConnected()) {
            connection.execute("CREATE TABLE IF NOT EXISTS example_table (id SERIAL PRIMARY KEY, name VARCHAR(255));")
            
            connection.execute("CREATE TABLE tags (id VARCHAR(64) NOT NULL, tag VARCHAR(255) NOT NULL, PRIMARY KEY (id, tag));")
            connection.execute("CREATE TABLE descriptions (id VARCHAR(64) NOT NULL, description VARCHAR(2056) NOT NULL, PRIMARY KEY id);")
            connection.execute("CREATE TABLE rates (id VARCHAR(64) NOT NULL, rate DOUBLE PRECISION NOT NULL, PRIMARY KEY id);")
            
            // Custom field types:
            //  0: string
            //  1: int
            //  2: double
            //  3: boolean
            connection.execute("CREATE TABLE custom_fields (id VARCHAR(64) NOT NULL, field_name VARCHAR(64) NOT NULL, type INT NOT NULL, value VARCHAR(2056) NOT NULL, PRIMARY KEY (id, field_name));")
        }
        
        return new Metadata(connection)
    }
    
    /**
     * Create a new metadata connection with the environment variables. Expected environment variables (with defaults) are:
     * - `QL_METADATA_DATABASE_HOST` (localhost)
     * - `QL_METADATA_DATABASE_PORT` (5432)
     * - `QL_METADATA_DATABASE` (metadata)
     * - `QL_METADATA_USER` (admin)
     * - `QL_METADATA_PASS` (pass)
     *
     * @returns[@type core.Metadata] The created metadata object
     */
    static fun defaultMetadata() {
        return createMetadata(Database.createDatabase(getEnv("QL_METADATA_DATABASE_HOST", "localhost"), getEnv("QL_METADATA_DATABASE_PORT", "5432").toInt(), getEnv("QL_METADATA_DATABASE", "metadata"), getEnv("QL_METADATA_USER", "admin"), getEnv("QL_METADATA_PASS", "pass")))
    }
    
    /**
     * Check if the metadata database connection is connected.
     *
     * @returns[@type boolean] true if connected, false if not
     */
    fun isConnected() {
        return _connection.isConnected()
    }
    
    /**
     * Get the parent object of the given object, if it has one.
     *
     * @param metaObject The object to get the parent of
     * @returns[@type core.Optional] The parent object, if it has one
     */
    fun _getParent(metaObject) {
        if (metaObject is song) {
            return Optional.fromValue(metaObject.getAlbum())
        } else if (metaObject is album) {
            return Optional.fromValue(metaObject.getArtist())
        }
        
        return Optional.fromEmpty()
    }
    
    /**
     * Get the hierarchy of IDs for the given object, inheriting from parent objects.
     *
     * @param metaObject The object to get the hierarchy for
     * @param[@type boolean] inherit Whether to inherit from parent objects. If false, only the given object's ID is returned in the array
     * @returns[@type list] The hierarchy of IDs for the object
     */
    fun _getHierarchyIds(metaObject, inherit) {
        string[] ids = [metaObject.getId()]
        
        if (!inherit) {
            return ids
        }
        
        Optional parentOptional = _getParent(metaObject)
        for (parentOptional.hasValue()) {
            ids.add(parentOptional.getValue().getId())
            parentOptional = _getParent(parentOptional.getValue())
        }
        
        return ids
    }
    
    /**
     * Construct a string of `?` placeholders for a prepared statement.
     *
     * @param[@type int] count The number of placeholders to construct
     * @returns[@type string] The constructed placeholder string
     */
    fun _constructPlaceholders(count) {
        string[] placeholders = []
        for (i..count) {
            placeholders.add("?")
        }
        
        return placeholders.join(", ")
    }
    
    /**
     * Get the tags for the given object, inheriting from parent objects. If no tags are found, an empty list is returned.
     *
     * @param metaObject The object to get tags for
     * @returns[@type list] The tags for the object, as a list of strings
     */
    fun getTags(metaObject) {
        return getTags(metaObject, true)
    }
    
    /**
     * Get the tags for the given object. If no tags are found, an empty list is returned.
     *
     * @param metaObject The object to get tags for
     * @param[@type boolean] inherit Whether to inherit from parent objects
     * @returns[@type list] The tags for the object, as a list of strings
     */
    fun getTags(metaObject, inherit) {
        string[] allIds = _getHierarchyIds(metaObject, inherit)
        
        // This is a little funky as this does formatting in the prepared statement string, however only the ? placeholders
        // are added in, and proper preparation of the statement still occurs.
        PreparedStatement statement = _connection.prepareStatement("SELECT id, tag FROM tags WHERE id in (%s)".format([_constructPlaceholders(allIds.size())]), allIds)
        Result result = _connection.fetchAll(statement)

        string[] tags = []

        if (!result.isSuccess()) {
            return tags
        }

        any[] rows = result.getValue()

        for (row : rows) {
            tags.add(row[1])
        }

        return tags
    }
    
    /**
     * Get the description for the given object, inheriting from parent objects. If no description is found, an empty
     * string is returned.
     *
     * @param metaObject The object to get the description for
     * @returns[@type string] The description for the object
     */
    fun getDescription(metaObject) {
        return getDescription(metaObject, true)
    }
    
    fun _constructDescriptionPlaceholder(count) {
        string[] placeholders = []
        for (i..count) {
            placeholders.add("(SELECT description FROM descriptions WHERE id = ? LIMIT 1)")
        }
        
        return placeholders.join(", ")
    }
    
    /**
     * Get the description for the given object. If no description is found, an empty string is returned.
     *
     * @param metaObject The object to get the description for
     * @param[@type boolean] inherit Whether to inherit from parent objects
     * @returns[@type string] The description for the object. If no description is found, an empty string is returned
     */
    fun getDescription(metaObject, inherit) {
        string[] allIds = _getHierarchyIds(metaObject, inherit)
        
        // Tries the first ID, if nothing is found, go to the next, etc.
        PreparedStatement statement = _connection.prepareStatement("SELECT COALESCE(%s) AS description".format([_constructDescriptionPlaceholder(allIds.size())]), allIds)
        Result result = _connection.fetchOne(statement)
        
        if (!result.isSuccess()) {
            return ""
        }
        
        return result.getValue()[0]
    }
    
    /**
     * Get the rating for the given object, inheriting from parent objects. If no rating is found, -1 is returned.
     *
     * @param metaObject The object to get the rating for
     * @returns[@type double] The rating for the object
     */
    fun getRating(metaObject) {
        return getRating(metaObject, true)
    }
    
    /**
     * Get the rating for the given object. If no rating is found, -1 is returned.
     *
     * @param metaObject The object to get the rating for
     * @param[@type boolean] inherit Whether to inherit from parent objects
     * @returns[@type double] The rating for the object
     */
    fun getRating(metaObject, inherit) {
        // TODO: Implement
    }
    
    /**
     * Get the custom field for the given object, returned as an Optional.
     *
     * @param metaObject The object to get the custom field for
     * @param field The field to get
     * @returns[@type core.Optional] The custom field for the object, if present. Actual value is whatever type it was
     *                               set to (e.g. string, int, etc.)
     */
    fun getCustomField(metaObject, field) {
        return getCustomField(metaObject, field, true)
    }
    
    /**
     * Get the custom field for the given object, returned as an Optional.
     *
     * @param metaObject The object to get the custom field for
     * @param field The field to get
     * @returns[@type core.Optional] The custom field for the object, if present. Actual value is whatever type it was
     *                               set to (e.g. string, int, etc.)
     */
    fun getCustomField(metaObject, field, inherit) {
        
    }
    
    /**
     * Get all custom fields for the given object, inheriting from parent objects.
     *
     * @param metaObject The object to get the custom fields for
     * @returns[@type core.Map] A map of all custom fields for the object, with the field name as the key and the value as the value
     */
    fun getAllCustomFields(metaObject) {
        return getAllCustomFields(metaObject, true)
    }
    
    /**
     * Get all custom fields for the given object.
     *
     * @param metaObject The object to get the custom fields for
     * @param[@type boolean] inherit Whether to inherit from parent objects
     * @returns[@type core.Map] A map of all custom fields for the object, with the field name as the key and the value as the value
     */
    fun getAllCustomFields(metaObject, inherit) {
        
    }
    
    /**
     * Set the tags for the given object.
     *
     * @param metaObject The object to set tags for
     * @param[@type list] tags The tags to set, as a list of strings
     */
    fun setTags(metaObject, tags) {
        // TODO: clear tags and add
    }
    
    /**
     * Add a tag to the given object.
     *
     * @param metaObject The object to add the tag to
     * @param[@type string] tag The tag to add
     */
    fun addTag(metaObject, tag) {
        PreparedStatement statement = _connection.prepareStatement("INSERT INTO tags (id, tag) VALUES (?, ?) ON CONFLICT DO NOTHING;", [metaObject.getId(), tag])
        _connection.update(statement)
    }
    
    /**
     * Remove a tag from the given object. If the tag is found in parent objects, remove it from there too.
     *
     * @param metaObject The object to remove the tag from
     * @param[@type string] tag The tag to remove
     */
    fun removeTag(metaObject, tag) {
        return removeTag(metaObject, tag, true)
    }
    
    /**
     * Remove a tag from the given object, inheriting from parent objects.
     *
     * @param metaObject The object to remove the tag from
     * @param[@type string] tag The tag to remove
     * @param[@type boolean] inherit Whether to inherit from parent objects
     */
    fun removeTag(metaObject, tag, inherit) {
        string[] allIds = _getHierarchyIds(metaObject, inherit)
        string[] placeholders = allIds
        placeholders.add(tag)
        
        PreparedStatement statement = _connection.prepareStatement("DELETE FROM tags WHERE id IN (%s) AND tag = ?;".format([_constructPlaceholders(allIds.size())]), placeholders)
        _connection.update(statement)
    }
    
    /**
     * Set the description for the given object.
     *
     * @param metaObject The object to set the description for
     * @param[@type string] description The description to set
     */
    fun setDescription(metaObject, description) {
        PreparedStatement statement = _connection.prepareStatement("INSERT INTO descriptions (id, description) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET description = EXCLUDED.description;", [metaObject.getId(), description])
        _connection.update(statement)
    }
    
    /**
     * Set the rating for the given object.
     *
     * @param metaObject The object to set the rating for
     * @param[@type double] rating The rating to set
     */
    fun setRating(metaObject, rating) {
        PreparedStatement statement = _connection.prepareStatement("INSERT INTO rates (id, rate) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET rate = EXCLUDED.rate;", [metaObject.getId(), rating])
        _connection.update(statement)
    }
    
    /**
     * Set the custom field for the given object. Custom field allowed types:
     * string, int, double, boolean
     *
     * @param metaObject The object to set the custom field for
     * @param field The field to set
     * @param value The value to set
     */
    fun setCustomField(metaObject, field, value) {
        int type = 0 // string
        
        if (value is int) {
            type = 1
        } else if (value is double) {
            type = 2
        } else if (value is boolean) {
            type = 3
        }
        
        PreparedStatement statement = _connection.prepareStatement("INSERT INTO custom_fields (id, field_name, type, value) VALUES (?, ?, ?, ?) ON CONFLICT (id, field_name) DO UPDATE SET type = EXCLUDED.type, value = EXCLUDED.value;", [metaObject.getId(), field, type, string(value)])
        _connection.update(statement)
    }
    
    /**
     * Set all custom fields for the given object. Custom field allowed types:
     * string, int, double, boolean, list (of the previous types)
     *
     * @param metaObject The object to set the custom fields for
     * @param[@type core.Map] fields A map of all custom fields for the object, with the field name as the key and the value as the value
     */
    fun setAllCustomFields(metaObject, fields) {
        
    }
    
    
}
