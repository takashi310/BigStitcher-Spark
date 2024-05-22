package net.preibisch.bigstitcher.spark.blk;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import net.imglib2.type.numeric.RealType;
import net.preibisch.bigstitcher.spark.util.DataTypeUtil;
import org.janelia.saalfeldlab.n5.*;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.blocks.PrimitiveBlocks;
import net.imglib2.type.NativeType;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import static net.preibisch.bigstitcher.spark.N5BlockValidateAndRetry.validateAndRetry;

public class N5Helper
{
	public static < T extends NativeType< T > > void saveBlock(
			final RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		if ( attributes != null )
		{
			final int[] size = new int[ source.numDimensions() ];
			Arrays.setAll( size, d -> ( int ) source.dimension( d ) );
			final DataBlock< ? > dataBlock = attributes.getDataType().createDataBlock( size, gridOffset );
			PrimitiveBlocks.of( source ).copy( source.minAsLongArray(), dataBlock.getData(), size );
			n5.writeBlock( dataset, attributes, dataBlock );

			if (attributes.getCompression() instanceof GzipCompression) {
				validateAndRetry(source, n5, dataset, gridOffset, dataBlock, 3, 3);
			}
		}
		else
		{
			throw new IOException( "Dataset " + dataset + " does not exist." );
		}
	}

}
