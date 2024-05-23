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
import java.util.zip.GZIPInputStream;

public class N5BlockValidateAndRetry {

    public static boolean ValidateN5Block(String blockPath) {
        boolean valid = true;
        try (FileInputStream fis = new FileInputStream(blockPath)) {
            // Skip the first 16 bytes
            if (fis.skip(16) != 16) {
                valid = false; // Unable to skip 16 bytes
            } else {
                try (GZIPInputStream gzis = new GZIPInputStream(fis)) {
                    byte[] buffer = new byte[8192];
                    while (gzis.read(buffer) != -1) {
                        // Reading to validate the stream
                    }
                }
                valid = true;
            }
        } catch (IOException e) {
            valid = false;
        }

        return valid;
    }
    public static <T extends net.imglib2.type.NativeType<T>> void validateAndRetry(
            net.imglib2.RandomAccessibleInterval<T> source,
            org.janelia.saalfeldlab.n5.N5Writer n5,
            String dataset,
            long[] gridOffset,
            T defaultValue,
            int[] blockSize,
            int retry,
            int wait) {

        final long[] gridPosition = new long[gridOffset.length];
        for (int d = 0; d < gridOffset.length; ++d)
            gridPosition[d] = gridOffset[d] / blockSize[d];
        GsonKeyValueN5Writer n5w = (GsonKeyValueN5Writer) n5;
        final String blockPath = n5w.absoluteDataBlockPath(N5URI.normalizeGroupPath(dataset), gridPosition);
        boolean valid = true;
        do {
            valid = ValidateN5Block(blockPath);
            if (!valid) {
                System.err.println( "The n5 block is corrupted. retrying... " + retry);
                try {
                    Thread.sleep(wait * 1000);
                } catch (InterruptedException e) {
                    System.err.println( "could not sleep" );
                }
                try {
                    N5Utils.saveNonEmptyBlock(source, n5, dataset, gridOffset, defaultValue);
                } catch (Exception e) {
                    valid = false;
                }
            }
            retry--;
        } while(!valid && retry > 0);
    }

    public static <T extends net.imglib2.type.NativeType<T>> void validateAndRetry(
            final RandomAccessibleInterval< T > source,
            final N5Writer n5,
            final String dataset,
            final long[] gridOffset,
            DataBlock< ? > dataBlock,
            int retry,
            int wait) {
        final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
        GsonKeyValueN5Writer n5w = (GsonKeyValueN5Writer) n5;
        final String blockPath = n5w.absoluteDataBlockPath(N5URI.normalizeGroupPath(dataset), dataBlock.getGridPosition());
        boolean valid = true;
        do {
            valid = ValidateN5Block(blockPath);
            if (!valid) {
                System.err.println( "The n5 block is corrupted. retrying... " + retry);
                try {
                    Thread.sleep(wait * 1000);
                } catch (InterruptedException e) {
                    System.err.println( "could not sleep" );
                }
                try {
                    n5.writeBlock( dataset, attributes, dataBlock );
                } catch (Exception e) {
                    valid = false;
                }
            }
            retry--;
        } while(!valid && retry > 0);
    }
}
