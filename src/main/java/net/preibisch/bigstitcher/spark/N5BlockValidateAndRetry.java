package net.preibisch.bigstitcher.spark;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
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
    public static <T extends net.imglib2.type.NativeType<T>> void validateAndRetry(
            net.imglib2.RandomAccessibleInterval<T> source,
            org.janelia.saalfeldlab.n5.N5Writer n5,
            String dataset,
            long[] gridOffset,
            T defaultValue,
            int[] blockSize) {
        int retry = RETRY_NUM;
        final long[] gridPosition = new long[gridOffset.length];
        for (int d = 0; d < gridOffset.length; ++d)
            gridPosition[d] = gridOffset[d] / blockSize[d];
        System.err.println( "offset "+ Arrays.toString(gridOffset) +" block_size " + Arrays.toString(blockSize));
        boolean valid = false;
        while(!valid && retry > 0) {
            valid = ValidateN5Block(n5, dataset, gridPosition);
            if (!valid) {
                System.err.println( "The n5 block "+ Arrays.toString(gridPosition) +" is corrupted. retrying... " + retry);
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
