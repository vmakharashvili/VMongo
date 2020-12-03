using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VMongo
{
    public class Sequence
    {
        [BsonId]
        [BsonRepresentation(MongoDB.Bson.BsonType.ObjectId)]
        [BsonElement]
        public string? Id { get; set; }

        public string? Name { get; set; }
        public string? Value { get; set; }

    }
}
