package net.imglib2.realtransform;

import java.util.Arrays;

import net.imglib2.RealLocalizable;
import net.imglib2.RealPositionable;

public class StackedRealTransform implements RealTransform {

	private final int nSourceDims;
	private final int nTargetDims;
	private final RealTransform[] transforms;

	protected double[] tmpSrc;
	protected double[] tmpTgt;

	public StackedRealTransform(final RealTransform... transforms) {

		this.transforms = transforms;
		int ns = 0;
		int nt = 0;

		int maxDim = -1;

		for (final RealTransform t : transforms) {
			ns += t.numSourceDimensions();
			nt += t.numTargetDimensions();

			maxDim = ns > maxDim ? ns : maxDim;
			maxDim = nt > maxDim ? nt : maxDim;
		}
		tmpSrc = new double[maxDim];
		tmpTgt = new double[maxDim];

		nSourceDims = ns;
		nTargetDims = nt;
	}

	@Override
	public int numSourceDimensions() {

		return nSourceDims;
	}

	@Override
	public int numTargetDimensions() {

		return nTargetDims;
	}

	@Override
	public void apply(final double[] source, final double[] target) {

		int startSrc = 0;
		int startTgt = 0;
		for (final RealTransform t : transforms) {

			System.arraycopy(source, startSrc, tmpSrc, 0, t.numSourceDimensions());
			t.apply(tmpSrc, tmpTgt);
			System.arraycopy(tmpTgt, 0, target, startTgt, t.numTargetDimensions());

			startSrc += t.numSourceDimensions();
			startTgt += t.numTargetDimensions();
		}
	}

	protected void localizeFromIndex(final RealLocalizable pt, final double[] arr, final int start, final int N) {

		for (int j = 0; j < N; j++)
			arr[j] = pt.getDoublePosition(start + j);
	}

	protected void positionFromIndex(final RealPositionable pt, final double[] arr, final int start, final int N) {

		for (int j = 0; j < N; j++)
			pt.setPosition(arr[j], start + j);
	}

	@Override
	public void apply(final RealLocalizable source, final RealPositionable target) {

		int startSrc = 0;
		int startTgt = 0;
		for (final RealTransform t : transforms) {

			localizeFromIndex(source, tmpSrc, startSrc, t.numSourceDimensions());
			t.apply(tmpSrc, tmpTgt);
			positionFromIndex(target, tmpTgt, startTgt, t.numTargetDimensions());

			startSrc += t.numSourceDimensions();
			startTgt += t.numTargetDimensions();
		}
	}

	@Override
	public RealTransform copy() {

		return new StackedRealTransform(
				Arrays.stream(transforms).map(RealTransform::copy).toArray(RealTransform[]::new));
	}
}
