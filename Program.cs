using BenchmarkDotNet;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using SmallestCSV;
using NReco.Csv;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Jobs;

public class Speed : IColumn
{
    static long GetDirectorySize(string folderPath)
    {
        long totalSize = 0;
        DirectoryInfo di = new DirectoryInfo(folderPath);

        foreach (FileInfo fi in di.EnumerateFiles("*.*", SearchOption.AllDirectories))
        {
            totalSize += fi.Length;
        }

        return totalSize;
    }
    public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
    {
#pragma warning disable CA1062
        var ourReport = summary.Reports.First(x => x.BenchmarkCase.Equals(benchmarkCase));
        long length = 0;
        if (File.Exists(CSVScan.filename))
        {
            length = new System.IO.FileInfo(CSVScan.filename).Length;
        }
        if (ourReport.ResultStatistics is null)
        {
            return "N/A";
        }
        var mean = ourReport.ResultStatistics.Mean;
        return $"{(length / ourReport.ResultStatistics.Mean):#####.00}";
    }

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style) => GetValue(summary, benchmarkCase);
    public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
    public bool IsAvailable(Summary summary) => true;

    public string Id { get; } = nameof(Speed);
    public string ColumnName { get; } = "Speed (GB/s)";
    public bool AlwaysShow { get; } = true;
    public ColumnCategory Category { get; } = ColumnCategory.Custom;
#pragma warning disable CA1805
    public int PriorityInCategory { get; } = 0;
#pragma warning disable CA1805
    public bool IsNumeric { get; } = false;
    public UnitType UnitType { get; } = UnitType.Dimensionless;
    public string Legend { get; } = "The speed in gigabytes per second";
}

[Config(typeof(Config))]

public class CSVScan
{

    public static string filename = "data/Table_1_Authors_career_2023_pubs_since_1788_wopp_extracted_202408_justnames.csv";
#pragma warning disable CA1812
    private sealed class Config : ManualConfig
    {
        public Config()
        {
            AddColumn(new Speed());
        }
    }

    [Benchmark]
    public int ScanFile()
    {
        int count = 0;
        using (var reader = new StreamReader(filename, System.Text.Encoding.UTF8))
        {
            while (reader.ReadLine() != null)
            {
                count++;
            }
        }
        return count;
    }

    [Benchmark]
    public List<string> ScanCsvHelper()
    {
        var matchingLines = new List<string>();

        using (var reader = new StreamReader(filename, System.Text.Encoding.UTF8))
        using (var csv = new CsvHelper.CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
        {
            // Skip the header row if your CSV has one
            if (!csv.Read())
            {
                throw new Exception("No records found in file.");
            }

            while (csv.Read())
            {
                // Assuming 'inst_name' is in the second column (index 1)
                if (csv[1].IndexOf("Harvard", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    matchingLines.Add(string.Join(",", csv.Parser.RawRecord));
                }
            }
        }

        return matchingLines;

    }
    [Benchmark]
    public List<string> ScanSmallestCSVParser()
    {
        var matchingLines = new List<string>();

        using var sr = new StreamReader(filename, System.Text.Encoding.UTF8);

        var parser = new SmallestCSV.SmallestCSVParser(sr);

        const bool removeEnclosingQuotes = false;
        List<string>? columns = parser.ReadNextRow(removeEnclosingQuotes: removeEnclosingQuotes);
        if (columns == null)
        {
            return matchingLines;
        }
        while (true)
        {
            columns = parser.ReadNextRow(removeEnclosingQuotes: removeEnclosingQuotes);
            if (columns == null)
            {
                break;
            }

            // Assuming 'inst_name' is in the second column (index 1)
            if (columns[1].IndexOf("Harvard", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                matchingLines.Add(string.Join(",", columns));
            }
        }
        return matchingLines;

    }

    [Benchmark]
    public List<string> ScanNReco()
    {
        var matchingLines = new List<string>();
        using (var streamRdr = new StreamReader(filename, System.Text.Encoding.UTF8))
        {
            var csvReader = new NReco.Csv.CsvReader(streamRdr, ",");
            while (csvReader.Read())
            {
                if (csvReader[1].IndexOf("Harvard", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    string packed = csvReader[0];
                    for (int i = 1; i < csvReader.FieldsCount; i++)
                    {
                        packed += "," + csvReader[i];
                    }
                    matchingLines.Add(packed);
                }
            }
        }
        return matchingLines;

    }

}

class Program
{
    public static void Main(string[] args)
    {
        var summary = BenchmarkRunner.Run<CSVScan>();
    }

}
