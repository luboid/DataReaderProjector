using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TestConsole.Model
{
    public class Customer
    {
        public Customer() { }

        public long? Id { get; set; }
        public string Name { get; set; }
        public string EGN { get; set; }

        public List<Address> Addresses { get; set; }
        public List<Phone> Phones { get; set; }
    }
}
