import "metadata:metadata.ql"

Metadata metadata = Metadata.createMetadata(Database.createDatabase("localhost", 5444, "metadata", "admin", "pass"))

//album myAlbum = "A Tear in the Fabric of Life" album by "Knocked Loose"
//metadata.addTag(myAlbum, "album_studio")
//print(myAlbum)

song mySong = "God Knows" by "Knocked Loose"

//metadata.addTag(mySong, "goated")

print("Tags for song: %s".format([metadata.getTags(mySong).join(", ")]))
