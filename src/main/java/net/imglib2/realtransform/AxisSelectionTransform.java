package net.imglib2.realtransform;

import java.util.Arrays;

import net.imglib2.RealLocalizable;
import net.imglib2.RealPositionable;

public class AxisSelectionTransform implements RealTransform {

	private final RealTransform transform;

	private final int[] axes;

	private final boolean unProject;

	private final RealInvertibleComponentMappingTransform permutation;

	private final RealTransformSequence seq;

	public AxisSelectionTransform(
			final RealTransform transform,
			final int[] axes,
			final int nd,
			final boolean unProject) {

		this.transform = transform;
		this.axes = axes;
		this.unProject = unProject;
		permutation = new RealInvertibleComponentMappingTransform(axes);

		seq = new RealTransformSequence();
		seq.add(permutation);
		seq.add(transform);
		if (unProject)
			seq.add(permutation.inverse());
	}

	public AxisSelectionTransform(final RealTransform transform, final int[] axes, final boolean unProject) {

		this(transform, axes, Arrays.stream(axes).max().getAsInt(), unProject);
	}

	@Override
	public int numSourceDimensions() {

		return permutation.numDimensions();
	}

	@Override
	public int numTargetDimensions() {

		return unProject ? numSourceDimensions() : transform.numTargetDimensions();
	}

	@Override
	public void apply(final double[] source, final double[] target) {

		seq.apply(source, target);
	}

	@Override
	public void apply(final RealLocalizable source, final RealPositionable target) {

		seq.apply(source, target);
	}

	@Override
	public RealTransform copy() {

		return new AxisSelectionTransform(transform, axes, unProject);
	}
}
