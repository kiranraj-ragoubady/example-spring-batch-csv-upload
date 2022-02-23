package com.example.springbatchexample.tasks;

import com.example.springbatchexample.models.DataRecord;
import org.hibernate.engine.jdbc.batch.spi.Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Handler;

@Configuration
@EnableBatchProcessing
public class BatchUpload {
    private static final Logger log = LoggerFactory.getLogger(BatchUpload.class);

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Value("${trade.file.input}")
    String inputFilePath;

    @Bean
    public FlatFileItemReader<DataRecord> reader()
    {
        //read file and get fields in order
        ArrayList<String> fields = new ArrayList<>();
        fields.add("TRADE_ID");
        fields.add("COUNTERPARTY");
        fields.add("MATURITY_DATE");
        //Create reader instance
        return new FlatFileItemReaderBuilder<DataRecord>().name("dataItemReader")
                .resource(new ClassPathResource(inputFilePath))
                .lineMapper(new CustomLineMapper(fields, ","))
                .linesToSkip(1)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter writer(DataSource dataSource) {
        ArrayList<String> columns = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {

        } catch (SQLException ex) {
            log.error("Problem retrieving columns from table..", ex);
        }

        ArrayList<String> fields = new ArrayList<>();
        fields.add("TRADE_ID");
        fields.add("COUNTERPARTY");
        fields.add("MATURITY_DATE");
        String queryColumns = "";
        String valueColumns = "";
        for(String field : fields) {
            queryColumns += field + ", ";
            valueColumns += ":" + field + ", ";
        }

        return new JdbcBatchItemWriterBuilder<DataRecord>()
                .itemSqlParameterSourceProvider(item -> {
                    MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();
                    mapSqlParameterSource.addValues(item.getDataMap());
                    return mapSqlParameterSource;
                })
                .sql("INSERT INTO TEMP_TRADE_STATS (" + queryColumns + ") VALUES (" + valueColumns + ")")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Job importDataJob(CompletionListener listener, Step step1) {
        return jobBuilderFactory.get("importDataJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }

    @Bean
    public Step step1(JdbcBatchItemWriter<DataRecord> writer) {
        return stepBuilderFactory.get("step1")
                .<DataRecord, DataRecord> chunk(10)
                .reader(reader())
//                .processor(processor())
                .writer(writer)
                .build();
    }

}
