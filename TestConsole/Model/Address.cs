using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TestConsole.Model
{
    public class Address
    {
        public Address() { }

        public long Id { get; set; }
        public string Kind { get; set; }
        public string City { get; set; }
        public string Zip { get; set; }

        public HintValue Type { get; set; }
    }
}
