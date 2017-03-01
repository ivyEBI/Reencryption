package uk.ac.ebi.ega;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Arrays;
import java.util.ArrayList;
import java.net.*;
import java.io.*;
import org.apache.commons.cli.*;
import uk.ac.ebi.ega.res.FileReencryption;
import java.sql.*;
import uk.ac.ebi.ega.JA5;
import uk.ac.ebi.ega.DataBase;
import uk.ac.ebi.ega.PropDb;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.tribble.index.tabix.*;
import htsjdk.tribble.index.*;
import htsjdk.variant.vcf.VCFRecordCodec;
import htsjdk.variant.vcf.*;
/**
 * Created by ivyding on 21/12/2016.
 */


public class gpg2Cip {
    private static final Logger LOGGER = Logger.getLogger(gpg2Cip.class.getName());
    static FileReencryption fileReencryption;
    static Path input;
    static Path outputDir;
    static String  serverUrl;
    static String originKey = "";
    static String originKeyFormat = "";
    static String destinationKey = "";
    static String destinationKeyFormat = "";
    static int bufferSize = 65536;
    static int timeout = 30;
    static String md5;
    static boolean useProxy;
    static boolean caculculateClientStats;
    static boolean skipDownload;
    static boolean writeToDisk;
    static String config;

    private static String getFileMd5(Path input) throws IOException{
        LOGGER.info("Getting the original file's md5.");
        String md5File =  input.toString() + ".md5";
        String md5 = new String(Files.readAllBytes(Paths.get(md5File)));
        return md5;
    }

    private static boolean compareMd5(String plainMd5, String DownloadMd5, String originMd5){
        LOGGER.info("Checking if the md5 after reencryption.");

        boolean test;
            if (originMd5.equals(plainMd5) && originMd5.equals(DownloadMd5)){test = true;}
            else{ test = false;}
            return test;
    }

    private static void run(Path input, Path outputDir, URL serverUrl, String originKey, String originKeyFormat, String destinationKey,
                       String destinationKeyFormat, int bufferSize, int timeout, boolean useProxy, boolean caculateClientStats, boolean skipDownload, boolean writeToDisk) throws IOException{
        FileReencryption fileReencryption = new FileReencryption(input, outputDir, serverUrl, originKey, originKeyFormat, destinationKey, destinationKeyFormat, bufferSize, timeout, useProxy, caculateClientStats, skipDownload, writeToDisk);
        LOGGER.info("Running the reencryption. ");
        boolean reencryptionSucceed = fileReencryption.run();
        if (reencryptionSucceed){
            if (compareMd5(fileReencryption.getPlainMd5(), fileReencryption.getDownloadedFileMd5(), getFileMd5(input))){
                System.out.print("Reencruption is done.");
            }
        }else{
            System.out.print("cannot reencrypt the " + input.toString());
            System.exit(1);
        }
        // vcf validation

    }

    public static void main(String args[]) throws java.net.MalformedURLException, java.io.IOException {
        LOGGER.info("Logger Name: " + LOGGER.getName());
        Options options = new Options();

        Option output = new Option("o", "output", true, "output file path" );
        output.setRequired(true);
        options.addOption(output);

        Option serverUrl = new Option("su","serverUrl", true, "server");
        serverUrl.setRequired(false);
        options.addOption(serverUrl);

        Option originalKey = new Option("or","originalKey",true, "original key");
        originalKey.setRequired(false);
        options.addOption(originalKey);

        Option originalKeyFormat = new Option("ork","originalKeyFormat", true, "original key format");
        originalKeyFormat.setRequired(true);
        options.addOption(originalKeyFormat);

        Option destinationKey = new Option("d", "destinationKey", true, "destination key");
        destinationKey.setRequired(false);
        options.addOption(destinationKey);

        Option destinationKeyFormat = new Option("dkf", "destinationKeyFormat", true, "destination key format");
        destinationKey.setRequired(true);
        options.addOption(destinationKeyFormat);

        Option bufferSize = new Option("b", "bufferSize", true, "buffer size");
        bufferSize.setRequired(true);
        options.addOption(bufferSize);

        Option timeout = new Option("t","timeout", true, "time out");
        timeout.setRequired(true);
        options.addOption(timeout);

        Option useProxy = new Option("u", "useProxy", true, "useProxy");
        useProxy.setRequired(true);
        options.addOption(useProxy);

        Option clientStats = new Option("cl", "clientStats", true, "clientStats");
        clientStats.setRequired(true);
        options.addOption(clientStats);

        Option skipDownload = new Option("s", "skipDownload", true, "skipDownload");
        skipDownload.setRequired(true);
        options.addOption(skipDownload);

        Option writeToDisk = new Option("w", "writeToDisk", true, "writeToDisk");
        writeToDisk.setRequired(true);
        options.addOption(writeToDisk);

        Option config = new Option("con","config", true, "config");
        writeToDisk.setRequired(true);
        options.addOption(config);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOGGER.log(Level.SEVERE, "Exception occur", e);
            formatter.printHelp("utility-name", options);
            System.exit(1);
            return;
        }
        String output1 = cmd.getOptionValue("output");
        String serverUrl1 = cmd.getOptionValue("serverUrl");
        URL urlObj = new URL(serverUrl1);
        String originalKey1 = cmd.getOptionValue("originalKey");
        String originalKeyFormat1 = cmd.getOptionValue("originalKeyFormat");
        String destinationKey1 = cmd.getOptionValue("destinationKey");
        String destinationKeyFormat1 = cmd.getOptionValue("destinationKeyFormat");
        String bufferSize1 = cmd.getOptionValue("bufferSize");
        String timeout1 = cmd.getOptionValue("timeout");
        String useProxy1 = cmd.getOptionValue("useProxy");
        String clientStats1 = cmd.getOptionValue("clientStats");
        String skipDownload1 = cmd.getOptionValue("skipDownload");
        String writeToDisk1 = cmd.getOptionValue("writeToDisk");
        String config1 = cmd.getOptionValue("config");
        System.out.println(config1);

        //read the property file
        LOGGER.info("Reading the config file.");
        PropDb auditObj = new PropDb();
        PropDb postObj = new PropDb();

        auditObj.loadProperty("auditReencryption","audit", config1);
        postObj.loadProperty("postgreReencryption","postgre",config1);

        LOGGER.info("Connecting to the audit database and postgresql database.");
        DataBase dbObjSelect = new DataBase(auditObj.getUrl(),auditObj.getUsername(),auditObj.getPassword());
        System.out.println(postObj.getUrl());
        System.out.println(postObj.getUsername());
        System.out.println(postObj.getPassword());
        DataBase dbObjInsert = new DataBase(postObj.getUrl(),postObj.getUsername(),postObj.getPassword());

        ResultSet theRS = null;
        try {
            dbObjSelect.getConnect();
            LOGGER.info("Getting data from the audit database.");
            theRS = dbObjSelect.getDataNoPara(auditObj.getQuery(0));
            dbObjInsert.getConnectPost();
            LOGGER.info("Inserting data to the postgresql database and VCF validating.");
            while (theRS.next()){
                String inputFolder = "/nfs/ega/public/box/";
                inputFolder = inputFolder.concat(theRS.getString(2));
                File newFile = new File (inputFolder);
                System.out.println(inputFolder);
                run(Paths.get(inputFolder),Paths.get(output1),urlObj,originalKey1,originalKeyFormat1,destinationKey1,destinationKeyFormat1,Integer.parseInt(bufferSize1),Integer.parseInt(timeout1),Boolean.parseBoolean(useProxy1),Boolean.parseBoolean(clientStats1),Boolean.parseBoolean(skipDownload1),Boolean.parseBoolean(writeToDisk1));
                String destinationFile = output1.concat(newFile.getName());
                System.out.println(destinationFile);
                System.out.println(postObj.getQuery(0));
                dbObjInsert.insertData(postObj.getQuery(0),new ArrayList<String> (Arrays.asList(theRS.getString(1).toString(), theRS.getString(2).toString(),destinationFile)));
                if (theRS.getString(2).contains("vcf")){
                    //File indexFile = new File (newFile.toString().concat(".tbi"));
                    //TabixIndex idx = IndexFactory.createTabixIndex(newFile,new VCFCodec(),TabixFormat.VCF, null);
                    //idx.write(indexFile);
                    try {
                        VCFFileReader vcfFile = new VCFFileReader(newFile);
                    }catch (Exception e){
                        System.out.println("The VCF validation doesn't pass.");
                    }
                }
            }



        }catch(SQLException e){
            LOGGER.log(Level.SEVERE, "Exception occur", e);
        }catch (ClassNotFoundException e){
            LOGGER.log(Level.SEVERE, "Exception occur", e);
        }
    }
}