using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TestConsole.Model
{
    public class Phone
    {
        public long Id { get; set; }
        public string Kind { get; set; }
        public string AreaCode { get; set; }
        public string Number { get; set; }
        public string Contact { get; set; }

        public HintValue Type { get; set; }
    }
}