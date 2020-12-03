namespace VMongo
{
    public interface IVMongoEntity<IdType>
    {
        IdType Id { get; set; }
    }
}
