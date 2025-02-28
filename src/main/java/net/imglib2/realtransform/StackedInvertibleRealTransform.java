package net.imglib2.realtransform;

import java.util.Arrays;

import net.imglib2.RealLocalizable;
import net.imglib2.RealPositionable;

public class StackedInvertibleRealTransform extends StackedRealTransform implements InvertibleRealTransform {

	private final InvertibleRealTransform[] transforms;

	public StackedInvertibleRealTransform(final InvertibleRealTransform... transforms) {

		super(transforms);
		this.transforms = transforms;
	}

	@Override
	public InvertibleRealTransform copy() {

		return new StackedInvertibleRealTransform(
				Arrays.stream(transforms).map(InvertibleRealTransform::copy).toArray(InvertibleRealTransform[]::new));
	}

	@Override
	public void applyInverse(final double[] source, final double[] target) {

		int startSrc = 0;
		int startTgt = 0;
		for (final InvertibleRealTransform t : transforms) {

			System.arraycopy(target, startTgt, tmpTgt, 0, t.numTargetDimensions());
			t.applyInverse(tmpSrc, tmpTgt);
			System.arraycopy(tmpSrc, 0, source, startSrc, t.numSourceDimensions());

			startSrc += t.numSourceDimensions();
			startTgt += t.numTargetDimensions();
		}
	}

	@Override
	public void applyInverse(final RealPositionable source, final RealLocalizable target) {

		int startSrc = 0;
		int startTgt = 0;
		for (final InvertibleRealTransform t : transforms) {

			localizeFromIndex(target, tmpTgt, startTgt, t.numTargetDimensions());
			t.applyInverse(tmpSrc, tmpTgt);
			positionFromIndex(source, tmpSrc, startSrc, t.numSourceDimensions());

			startSrc += t.numSourceDimensions();
			startTgt += t.numTargetDimensions();
		}
	}

	@Override
	public InvertibleRealTransform inverse() {

		// TODO Auto-generated method stub
		return null;
	}
}
