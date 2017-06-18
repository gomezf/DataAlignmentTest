using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Microsoft.VisualBasic.FileIO;
using Storm.DataModel;

namespace DataAlignmentTest
{
    class Program
    {
        const string HDTH = "DrillBoreHole.TD";
        const string BD = "DepthMonitoring.RBD";       // Bid Depth
        const string RIG_STATE = "DepthMonitoring.ACTIVITY";  // RigState
        const string BPOS = "HoistingSystem.HKH";        // BPOS

        static int globalCounter = 0;

        // Multi-well channel container
        static Dictionary<string, Dictionary<string, Channel>> _cacheDicForAllChannels = new Dictionary<string, Dictionary<string, Channel>>();
        // Multi-well list containing sorted list indexed by master channel time and containing times for all channels to align
        static Dictionary<string, SortedList<long, Dictionary<string, long>>> _alignedIndex = new Dictionary<string, SortedList<long, Dictionary<string, long>>>();
        static List<string> channelsNeedAligned = new List<string>();
        static long tolerance = (new TimeSpan(0, 0, 0, 1)).Ticks;  //1s
        static string masterChannelName = RIG_STATE;
   

        static void Main(string[] args)
        {
            //Dictionary<DateTime, string> numcoll = new Dictionary<DateTime, string> { };

            //DateTime refTime = DateTime.Now;
            //numcoll.Add(refTime,"Now");
            //numcoll.Add(refTime.AddSeconds(1), "Now+1");
            //numcoll.Add(refTime.AddSeconds(2), "Now+2");
            //numcoll.Add(refTime.AddSeconds(3), "Now+3");

            //for (int x=0; x < 4; x++)
            //{
            //    Console.WriteLine(numcoll.ElementAt(x));

            //}

            //Console.WriteLine(numcoll[refTime]);
            //Console.WriteLine(numcoll.ContainsKey(refTime));
            //Console.WriteLine(numcoll.ContainsValue("Now"));
            //string num;
            //numcoll.TryGetValue(refTime.AddMinutes(1),out num);
            //Console.WriteLine("TtryGetValue returned:  {0}",num);


            // List of channels needing alignment
           
            channelsNeedAligned.Add(RIG_STATE);
            channelsNeedAligned.Add(BPOS);
            channelsNeedAligned.Add(HDTH);
            channelsNeedAligned.Add(BD);

            // Create dictionaries for channels for a specific well
            _cacheDicForAllChannels.Add("Well_1", new Dictionary<string, Channel>());
            _alignedIndex.Add("Well_1", new SortedList<long, Dictionary<string, long>>());

            string csv_file_path = @"d:\software\DataAlignmentTest\DataAlignmentTest\DatasetForTestingQueueReader.csv";
            Console.WriteLine("Reading:  csv_file_path");

            DataTable queueTable = ReadQueueTableFromCSV(csv_file_path);
           

            //Data streamer ..........

            for (int i = 0; i < queueTable.Rows.Count; i++)
            {

                //Console.WriteLine("{0,-20:s} {1,-30} {2}",queueTable.Rows[i]["Time"], queueTable.Rows[i]["Channels"], queueTable.Rows[i]["Value"]);
                executeTuple(queueTable.Rows[i].Field<DateTime>("Time"), queueTable.Rows[i].Field<string>("Channel"), queueTable.Rows[i].Field<Single>("Value"));
                
            }
            
            Console.WriteLine("Rows:  {0}", queueTable.Rows.Count);
            Console.WriteLine("Filtered:  {0}", globalCounter);
            Console.WriteLine("Finished!");
            Console.ReadLine();

        }


        private static void executeTuple(DateTime ChannelTime, string ChannelName, Single ChannelValue)
        {
            long ChannelTimeTicks = ChannelTime.Ticks;
            
           if (channelsNeedAligned.Contains(ChannelName))  // Check that received channel is one that needs to be aligned otherwise ignore
            {

                var channelsForCurrentWell = _cacheDicForAllChannels["Well_1"];
                if (!channelsForCurrentWell.ContainsKey(ChannelName))             // Check if channel is cached otherwise creates container 
                    channelsForCurrentWell.Add(ChannelName, new Channel(ChannelName));

                channelsForCurrentWell[ChannelName].AddChannelValue(ChannelTimeTicks, ChannelValue); // Cache current value
                Console.WriteLine("Channel cached:  {0,-20:s} {1,-30} {2}", ChannelTime, ChannelName, ChannelValue);

                // Call chanel aligment here ....
                AlignData(channelsForCurrentWell);

                globalCounter++;
            }
        }

        private static void AlignData(Dictionary <string,Channel> channels)  //_cacheDicForAllChannels(for a well)
        {
            // Only execute if channel is masterChannel - RigState
            if (!channels.ContainsKey(masterChannelName))
                return;

            var masterChannel = channels[masterChannelName];
            var masterIndexes = masterChannel.GetIndex();
            var alignIndexForCurrentWell = _alignedIndex["Well_1"];

            foreach (var index in masterIndexes)
            {
                if (!alignIndexForCurrentWell.ContainsKey(index))
                {
                    alignIndexForCurrentWell.Add(index, new Dictionary<string, long>());
                    alignIndexForCurrentWell[index].Add(masterChannelName, index);
                }
                foreach (var channelName in channelsNeedAligned)
                {

                    if (channelName == masterChannelName || !channels.ContainsKey(channelName) || alignIndexForCurrentWell[index].ContainsKey(channelName))
                        continue;
                    var foundIndex = channels[channelName].FindIndexWithTolanrance(index, tolerance);
                    if (foundIndex > 0)
                    {
                        alignIndexForCurrentWell[index].Add(channelName, foundIndex);
                    }
                }
            }


        }

        private static void Compute()
        {

        }


        private static DataTable ReadQueueTableFromCSV(string csv_file_path)
        {
            DataTable csvData = new DataTable();

            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    string[] colFields = csvReader.ReadFields();

                    // time column
                    csvData.Columns.Add(colFields[0], typeof(string));

                    // time column
                    csvData.Columns.Add(colFields[1], typeof(DateTime));

                    // Values column
                    csvData.Columns.Add(colFields[2], typeof(Single));

                    while (!csvReader.EndOfData)
                    {
                        csvData.Rows.Add(csvReader.ReadFields()); 
                    }

                }
            }

            catch (Exception ex)
            {
                Console.WriteLine("Exception when reading file {0}", ex.Message);

            }

            return (csvData);
        
         }
    
        private static DataTable GetDataTableFromCSVFile(string csv_file_path)
        {
            DataTable csvData = new DataTable();

            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    string[] colFields = csvReader.ReadFields();

                    //foreach (string column in colFields)
                    //{
                    //    DataColumn datecolumn = new DataColumn(column);
                    //    datecolumn.AllowDBNull = true;
                    //    csvData.Columns.Add(datecolumn);

                    //}

                    // time column
                    csvData.Columns.Add(colFields[0], typeof(DateTime));

                    // Values column
                    csvData.Columns.Add(colFields[1], typeof(Single));

                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        // making empty value as Null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }

                           
                        }
                        csvData.Rows.Add(fieldData);
                    }


                }
            }

            catch (Exception ex)
            {
                Console.WriteLine("Exception when reading file {0}", ex.Message);
            }

            return csvData;
        }

        private static void findTest(DataTable table)
        {
            // DataTable csvData = GetDataTableFromCSVFile(csv_file_path);

            //   Console.WriteLine("Value for Activity 0:  {0}  {1}", DateTime.Parse(csvData.Rows[0]["Time.ACTIVITY"].ToString()),csvData.Rows[0]["ACTIVITY"]);

            //string expression;
            //DateTime tempTime = DateTime.Parse("2017/06/16 14:00:00");
            //expression = "Time.ACTIVITY >= '" + tempTime + "' AND Time.ACTIVITY <= '" + tempTime.AddSeconds(9.1) + "'"; 

            ////expression = "ACTIVITY = '2'";

            //DataRow[] foundRows;

            //// Use the Select method to find all rows matching the filter.
            //foundRows = csvData.Select(expression);

            // Print column 0 of each returned row.
        }

    }
}
