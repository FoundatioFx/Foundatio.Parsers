using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace Foundatio.Parsers.SqlQueries.Tests;

public class SampleContext : DbContext {
    public SampleContext(DbContextOptions<SampleContext> options) : base(options) { }
    public DbSet<Employee> Employees => Set<Employee>();
    public DbSet<Company> Companies => Set<Company>();
    public DbSet<DataDefinition> DataDefinitions => Set<DataDefinition>();
    public DbSet<DataValue> DataValues => Set<DataValue>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Employee
        modelBuilder.Entity<Employee>().HasIndex(e => new { e.FullName, e.Title });

        // Company
        modelBuilder.Entity<Company>().HasIndex(e => new { e.Name });

        // DataDefinition
        modelBuilder.Entity<DataDefinition>().Property(c => c.DataType).IsRequired();
        modelBuilder.Entity<DataDefinition>().HasIndex(c => new { c.CompanyId, c.Key }).IsUnique();

        // DataValue
        modelBuilder.Entity<DataValue>().HasIndex(c => new { c.DataDefinitionId, c.CompanyId, c.EmployeeId }).HasFilter(null).IsUnique();
        modelBuilder.Entity<DataValue>().Property(e => e.StringValue).HasMaxLength(4000).IsSparse();
        modelBuilder.Entity<DataValue>().Property(e => e.DateValue).IsSparse();
        modelBuilder.Entity<DataValue>().Property(e => e.MoneyValue).IsSparse().HasColumnType("money").HasPrecision(2);
        modelBuilder.Entity<DataValue>().Property(e => e.BooleanValue).IsSparse();
        modelBuilder.Entity<DataValue>().Property(e => e.NumberValue).HasColumnType("decimal").HasPrecision(15,3).IsSparse();
        modelBuilder.Entity<DataValue>().HasIndex(e => new { e.StringValue, e.DateValue, e.MoneyValue, e.BooleanValue, e.NumberValue });
    }
}

public class Employee {
    public int Id { get; set; }
    public string FullName { get; set; }
    public string Title { get; set; }
    public List<Company> Companies { get; set; }
    public List<DataValue> DataValues { get; set; }
    public DateTime Created { get; set; } = DateTime.Now;
}

public class Company {
    public int Id { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
    public List<Employee> Employees { get; set; }
    public List<DataDefinition> DataDefinitions { get; set; }
}

public class DataValue
{
    public int Id { get; set; }
    public int DataDefinitionId { get; set; }
    public int CompanyId { get; set; }
    public int EmployeeId { get; set; }

    // store the values separately as sparse columns for querying purposes
    public string StringValue { get; set; }
    public DateTime? DateValue { get; set; }
    public decimal? MoneyValue { get; set; }
    public bool? BooleanValue { get; set; }
    public decimal? NumberValue { get; set; }

    public DataDefinition Definition { get; set; } = null;

    public object GetValue(DataType? dataType = null)
    {
        if (!dataType.HasValue && Definition != null)
            dataType = Definition.DataType;

        if (dataType.HasValue)
        {
            return dataType switch
            {
                DataType.String => StringValue,
                DataType.Date => DateValue,
                DataType.Number => NumberValue,
                DataType.Boolean => BooleanValue,
                DataType.Money => MoneyValue,
                DataType.Percent => NumberValue,
                _ => null
            };
        }

        if (MoneyValue.HasValue)
            return MoneyValue.Value;
        if (BooleanValue.HasValue)
            return BooleanValue.Value;
        if (NumberValue.HasValue)
            return NumberValue.Value;
        if (DateValue.HasValue)
            return DateValue.Value;

        return StringValue ?? null;
    }

    public void ClearValue()
    {
        StringValue = null;
        DateValue = null;
        NumberValue = null;
        BooleanValue = null;
        MoneyValue = null;
    }

    public void SetValue(object value, DataType? dataType = null)
    {
        ClearValue();

        if (value == null)
            return;

        switch (dataType ?? Definition!.DataType)
        {
            case DataType.String:
                StringValue = value.ToString();
                break;
            case DataType.Date:
                if (DateTime.TryParse(value.ToString(), out DateTime dateResult))
                    DateValue = dateResult;
                break;
            case DataType.Number:
            case DataType.Percent:
                if (Decimal.TryParse(value.ToString(), out decimal numberResult))
                    NumberValue = numberResult;
                break;
            case DataType.Boolean:
                if (Boolean.TryParse(value.ToString(), out bool boolResult))
                    BooleanValue = boolResult;
                break;
            case DataType.Money:
                if (Decimal.TryParse(value.ToString(), out decimal decimalResult))
                    MoneyValue = decimalResult;
                break;
        }
    }

    // relationships
    [DeleteBehavior(DeleteBehavior.NoAction)]
    public Employee Employee { get; set; } = null;
}

public class DataDefinition
{
    public int Id { get; set; }
    public int CompanyId { get; set; }

    public DataType DataType { get; set; }
    public string Key { get; set; } = String.Empty;

    // relationships
    [DeleteBehavior(DeleteBehavior.Cascade)]
    public Company Company { get; set; } = null;
}

public enum DataType
{
    String,
    Number,
    Boolean,
    Date,
    Money,
    Percent
}
