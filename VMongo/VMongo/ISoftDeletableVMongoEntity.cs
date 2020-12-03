namespace VMongo
{
    public interface ISoftDeletableVMongoEntity<IdType>
    {
        IdType Id { get; set; }
        bool IsDeleted { get; set; }
    }
}
