package net.preibisch.bigstitcher.spark.util;

import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalRealInterval;
import net.imglib2.realtransform.AffineTransform3D;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.PairwiseStitchingResult;

import java.io.Serializable;

public class SerializablePairwiseStitchingResult implements Serializable
{
    private static final long serialVersionUID = -8920256594391301778L;

    final int[][][] pair; // Pair< Group<ViewId>, Group<ViewId> > pair;
    final double[][] matrix = new double[3][4]; //AffineTransform3D transform;
    final double[] min, max; //final RealInterval boundingBox;
    final double r;
    final double hash;

    public SerializablePairwiseStitchingResult( final PairwiseStitchingResult<ViewId> result )
    {
        this.r = result.r();
        this.hash = result.getHash();
        this.min = result.getBoundingBox().minAsDoubleArray();
        this.max = result.getBoundingBox().maxAsDoubleArray();
        this.pair = Spark.serializeGroupedViewIdPairForRDD( result.pair() );
        ((AffineTransform3D)result.getTransform()).toMatrix( matrix );
    }

    public PairwiseStitchingResult< ViewId > deserialize()
    {
        final AffineTransform3D t = new AffineTransform3D();
        t.set( matrix );

        return new PairwiseStitchingResult<>(
                Spark.deserializeGroupedViewIdPairForRDD( pair ),
                new FinalRealInterval(min, max),
                t,
                r,
                hash );
    }
}