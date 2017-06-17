using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Microsoft.VisualBasic.FileIO;

namespace DataAlignmentTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string csv_file_path = @"d:\software\DataAlignmentTest\DataAlignmentTest\PrismQueue_Jun16_RigState.csv";
            Console.WriteLine("Reading:  csv_file_path");
            DataTable csvData = GetDataTableFromCSVFile(csv_file_path);

            Console.WriteLine("Value for Activity 0:  {0}  {1}", DateTime.Parse(csvData.Rows[0]["Time.ACTIVITY"].ToString()),csvData.Rows[0]["ACTIVITY"]);

            // Presuming the DataTable has a column named Date.
            string expression;
            expression = "Time.ACTIVITY >= '" + DateTime.Parse("2017/06/16 14:00:00") + "' AND Time.ACTIVITY <= '" + DateTime.Parse("2017/06/16 14:10:00") + "'"; 

            //expression = "ACTIVITY = '2'";
            DataRow[] foundRows;

            // Use the Select method to find all rows matching the filter.
            foundRows = csvData.Select(expression);
       
            // Print column 0 of each returned row.
            for (int i = 0; i < foundRows.Length; i++)
            {
                Console.WriteLine("{0}  {1}",foundRows[i][0], foundRows[i][1]);
            }
            
            Console.WriteLine("Found:  {0}", foundRows.Length);
            Console.WriteLine("Finished!");
            Console.ReadLine();

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

    }
}
