package com.taosdata.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DataGenerator {
    /*
     * to simulate the change action of humidity The valid range of humidity is
     * [0, 100]
     */
    public static class ValueGen {
        int center;
        int range;
        Random rand;

        public ValueGen(final int center, final int range) {
            this.center = center;
            this.range = range;

            this.rand = new Random();
        }

        double next() {
            double v = this.rand.nextGaussian();
            if (v < -3) {
                v = -3;
            }

            if (v > 3) {
                v = 3;
            }

            return (this.range / 3.00) * v + center;
        }
    }

    // data scale
    private static int timestep = 1000; // sample time interval in milliseconds

    private static long dataStartTime = 1563249700000L;
    private static AtomicInteger deviceId = new AtomicInteger(0);
    private static String tagPrefix = "dev_";

    // MachineNum RowsPerMachine MachinesInOneFile
    public static void main(final String args[]) {
        int numOfDevice = 10000;
        int numOfFiles = 100;
        int rowsPerDevice = 10000;
        String directory = "~/";

        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-numOfDevices")) {
                if (i < args.length - 1) {
                    numOfDevice = Integer.parseInt(args[++i]);
                } else {
                    System.out.println("'-numOfDevices' requires a parameter, default is 10000");
                }
            } else if (args[i].equalsIgnoreCase("-numOfFiles")) {
                if (i < args.length - 1) {
                    numOfFiles = Integer.parseInt(args[++i]);
                } else {
                    System.out.println("'-numOfFiles' requires a parameter, default is 100");
                }
            } else if (args[i].equalsIgnoreCase("-rowsPerDevice")) {
                if (i < args.length - 1) {
                    rowsPerDevice = Integer.parseInt(args[++i]);
                } else {
                    System.out.println("'-rowsPerDevice' requires a parameter, default is 10000");
                }
            } else if (args[i].equalsIgnoreCase("-dataDir")) {
                if (i < args.length - 1) {
                    directory = args[++i];
                } else {
                    System.out.println("'-dataDir' requires a parameter, default is ~/testdata");
                }
            }
        }

        System.out.println("parameters");
        System.out.printf("----dataDir:%s\n", directory);
        System.out.printf("----numOfFiles:%d\n", numOfFiles);
        System.out.printf("----numOfDevice:%d\n", numOfDevice);
        System.out.printf("----rowsPerDevice:%d\n", rowsPerDevice);

        final int numOfDevPerFile = numOfDevice / numOfFiles;
        final long ts = dataStartTime;

        // deviceId, time stamp, humid(int), temp(double), tagString(dev_deviceid)
        final int humidityDistRadius = 35;
        final int tempDistRadius = 17;
        final ThreadPoolExecutor threadPoolExecutor =new ThreadPoolExecutor(4, 8, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(10000));
        for (int i = 0; i < numOfFiles; ++i) { // prepare the data file
            final String copyDir = directory;
            final int copyRowsPerDevice = rowsPerDevice;
            final int copynumOfDevPerFile = numOfDevPerFile;
            final int copyhumidityDistRadius =humidityDistRadius;
            final int copytempDistRadius =tempDistRadius;
            final int copyI = i;
            threadPoolExecutor.execute(new Runnable(){
                 @Override
                public void run() {
            try {
                 String path = copyDir;
                path += "/testdata" + String.valueOf(copyI) + ".csv";
                getDataInOneFile(path, copyRowsPerDevice, copynumOfDevPerFile, copyhumidityDistRadius, copytempDistRadius);
            } catch (IOException e) {
                e.printStackTrace();
            }
         }});
        }
        threadPoolExecutor.shutdown();
    }

    private static void getDataInOneFile(final String path, final int rowsPerDevice, final int num, final int humidityDistRadius, final int tempDistRadius) throws IOException {
        final DecimalFormat df = new DecimalFormat("0.0000");
        final long startTime = dataStartTime;

        final FileWriter fw = new FileWriter(new File(path));
        final BufferedWriter bw = new BufferedWriter(fw);

        for (int i = 0; i < num; ++i) {
             int devid =deviceId.getAndAdd(1);

            final Random rand = new Random();
            double centralVal = Math.abs(rand.nextInt(100));
            if (centralVal < humidityDistRadius) {
                centralVal = humidityDistRadius;
            }

            if (centralVal + humidityDistRadius > 100) {
                centralVal = 100 - humidityDistRadius;
            }

            final DataGenerator.ValueGen humidityDataGen = new DataGenerator.ValueGen((int) centralVal, humidityDistRadius);
            dataStartTime = startTime;

            centralVal = Math.abs(rand.nextInt(22));
            final DataGenerator.ValueGen tempDataGen = new DataGenerator.ValueGen((int) centralVal, tempDistRadius);

            for (int j = 0; j < rowsPerDevice; ++j) {
                final int humidity = (int) humidityDataGen.next();
                final double temp = tempDataGen.next();
                final int deviceGroup = devid % 100;

                final StringBuffer sb = new StringBuffer();
                sb.append(devid).append(" ").append(tagPrefix).append(devid).append(" ").append(deviceGroup)
                        .append(" ").append(dataStartTime).append(" ").append(humidity).append(" ")
                        .append(df.format(temp));
                bw.write(sb.toString());
                bw.write("\n");

                dataStartTime += timestep;
            }
        }

        bw.close();
        fw.close();
        System.out.printf("file:%s generated\n", path);
    }
}
