package net.preibisch.bigstitcher.spark;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.preibisch.bigstitcher.spark.util.DataTypeUtil;
import org.janelia.saalfeldlab.n5.*;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

public class N5BlockValidateAndRetry {
    public static int RETRY_NUM = 0;
    public static int WAIT_TIME = 5;

    public static boolean ValidateN5Block(final N5Writer n5,
                                          final String dataset,
                                          final long[] blockPosition) {
        try {
            // Read the specific block
            DataBlock<?> block = n5.readBlock(dataset, n5.getDatasetAttributes(dataset), blockPosition);

            // Validate the block
            if (block == null) {
                System.out.println("Block does not exist.");
                return false;
            }

        } catch (Exception e) {
            return false;
        }

        return true;
    }

    public static boolean ValidateN5Blocks(net.imglib2.RandomAccessibleInterval source,
                                           org.janelia.saalfeldlab.n5.N5Writer n5,
                                           String dataset,
                                           long[] gridOffset,
                                           int retry) {
        final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
        final int n = source.numDimensions();
        final long[] max = Intervals.maxAsLongArray(source);
        final long[] offset = new long[n];
        final long[] gridPosition = new long[n];
        final int[] blockSize = attributes.getBlockSize();
        final int[] intCroppedBlockSize = new int[n];
        final long[] longCroppedBlockSize = new long[n];

        for (int d = 0; d < n;) {
            for (int dd = 0; dd < max.length; ++dd) {
                longCroppedBlockSize[dd] = Math.min(blockSize[dd], max[dd] - offset[dd] + 1);
                intCroppedBlockSize[dd] = (int)longCroppedBlockSize[dd];
                gridPosition[dd] = offset[dd] / blockSize[dd] + gridOffset[dd];
            }
            System.out.println( "validating block "+ Arrays.toString(gridPosition));
            if (!ValidateN5Block(n5, dataset, gridPosition)) {
                System.err.println( "The n5 block "+ Arrays.toString(gridPosition) +" is corrupted. retrying... " + retry);
                return false;
            }

            for (d = 0; d < n; ++d) {
                offset[d] += blockSize[d];
                if (offset[d] <= max[d])
                    break;
                else
                    offset[d] = 0;
            }
        }

        return true;
    }

    public static <T extends net.imglib2.type.NativeType<T>> void validateAndRetry(
            net.imglib2.RandomAccessibleInterval<T> source,
            org.janelia.saalfeldlab.n5.N5Writer n5,
            String dataset,
            long[] gridOffset,
            T defaultValue) {
        int retry = RETRY_NUM;
        boolean valid = false;
        while(!valid && retry > 0) {
            valid = ValidateN5Blocks(source, n5, dataset, gridOffset, retry);
            if (!valid) {
                try {
                    Thread.sleep(WAIT_TIME * 1000);
                } catch (InterruptedException e) {
                    System.err.println( "validateAndRetry: failed to sleep" );
                }
                try {
                    N5Utils.saveNonEmptyBlock(source, n5, dataset, gridOffset, defaultValue);
                } catch (Exception e) {
                    valid = false;
                }
            }
            retry--;
        }
    }

    public static <T extends net.imglib2.type.NativeType<T>> void validateAndRetry(
            final RandomAccessibleInterval< T > source,
            final N5Writer n5,
            final String dataset,
            DataBlock< ? > dataBlock) {
        int retry = RETRY_NUM;
        final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
        boolean valid = false;
        while(!valid && retry > 0) {
            System.out.println( "validating block "+ Arrays.toString(dataBlock.getGridPosition()));
            valid = ValidateN5Block(n5, dataset, dataBlock.getGridPosition());
            if (!valid) {
                System.err.println( "The n5 block "+ Arrays.toString(dataBlock.getGridPosition()) +" is corrupted. retrying... " + retry);
                try {
                    Thread.sleep(WAIT_TIME * 1000);
                } catch (InterruptedException e) {
                    System.err.println( "validateAndRetry: failed to sleep" );
                }
                try {
                    n5.writeBlock( dataset, attributes, dataBlock );
                } catch (Exception e) {
                    valid = false;
                }
            }
            retry--;
        }
    }
}
