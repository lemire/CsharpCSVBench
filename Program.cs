using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using CsvHelper.Configuration;
using nietras.SeparatedValues;
using System.Globalization;
using System.Text;

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
        return $"{(length / ourReport.ResultStatistics.Mean),6:F3}";
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

[MemoryDiagnoser]
[HideColumns("Gen0", "Gen1", "Gen2")]
public class CSVScan
{

    public static string university = "Harvard";

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
    public List<string> ScanSep()
    {
        var matchingLines = new List<string>();
        using var reader = Sep.Reader().FromFile(filename);
        var colIndex = reader.Header.IndexOf("inst_name");
        foreach (var row in reader)
        {
            if (row[colIndex].Span.Contains(university, StringComparison.OrdinalIgnoreCase))
            {
                matchingLines.Add(row.Span.ToString());
            }
        }
        return matchingLines;
    }

    [Benchmark]
    public List<string> ScanFileRaw()
    {
        var matchingLines = new List<string>();


        using (FileStream fileStream = new FileStream(filename, FileMode.Open, FileAccess.Read))
        {
            // Create a buffer that can hold 4kB at a time
            // It needs to be much (e.g., 2x or more) larger than the university name
            byte[] buffer = new byte[4 * 1024];
            Span<byte> harvardBytes = Encoding.UTF8.GetBytes(university);
            var tailbytes = harvardBytes.Slice(1);
            int bytesRead;
            // Read the file in blocks
            int offset = 0;
            while ((bytesRead = fileStream.Read(buffer, offset, buffer.Length - offset)) > 0)
            {
                bytesRead += offset;
                offset = 0;
                for (int i = 0; i <= bytesRead - harvardBytes.Length;)
                {
                    i = Array.IndexOf(buffer, (byte)harvardBytes[0], i, bytesRead - harvardBytes.Length - i);
                    if (i < 0)
                    {
                        break;
                    }
                    Span<byte> region = buffer.AsSpan(i + 1, harvardBytes.Length - 1);
                    if (region.SequenceEqual(tailbytes))
                    {
                        var start = i;
                        var end = i + harvardBytes.Length;
                        while (start > 0 && buffer[start - 1] != '\n') { start--; }
                        while (end + 1 < bytesRead && buffer[end + 1] != '\n') { end++; }
                        string line = Encoding.UTF8.GetString(buffer, start, end - start + 1);
                        matchingLines.Add(line);
                        i += harvardBytes.Length;
                    }
                    else
                    {
                        i++;
                    }
                }

                for (int i = bytesRead - harvardBytes.Length + 1; i <= bytesRead; i++)
                {
                    Span<byte> region = buffer.AsSpan(i, bytesRead - i);
                    if (harvardBytes.StartsWith(region))
                    {
                        Array.Copy(buffer, i, buffer, 0, region.Length);
                        offset = region.Length;
                        break;
                    }
                }
            }
        }
        return matchingLines;
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
                if (csv[1]?.IndexOf(university, StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    matchingLines.Add(string.Join(",", csv.Parser.RawRecord).Trim());
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

        const bool removeEnclosingQuotes = true;
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
            if (columns[1].IndexOf(university, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                matchingLines.Add(string.Join(",", columns).Trim());
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
                if (csvReader[1].IndexOf(university, StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    string packed = csvReader[0];
                    for (int i = 1; i < csvReader.FieldsCount; i++)
                    {
                        packed += "," + csvReader[i];
                    }
                    matchingLines.Add(packed.Trim());
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
        var summary = BenchmarkRunner.Run<CSVScan>(args: args);
    }

}
