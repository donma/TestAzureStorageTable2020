using System;
using System.Collections.Generic;
using System.Text;

namespace TestAzureStorageTable2020
{
    public class User : DTableEntity
    {

        public string Name { get; set; }

        public Int32? Age { get; set; }

        public DateTime Create { get; set; }

        public List<User> Friends { get; set; }
        public string Meta { get; set; }

        public DateTime? LastLoginDate { get; set; }
        public User()
        {
            Friends = new List<User>();
        }
        public User(string classId, string userId)
        {
            PartitionKey = classId;
            RowKey = userId;
            Friends = new List<User>();


        }
    }
}
