/**
 * Copyright (c) 2017-2021, Saalfeld lab, HHMI Janelia
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 *  list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.imglib2;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import net.imglib2.Cursor;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineTransform;
import net.imglib2.realtransform.DisplacementFieldTransform;
import net.imglib2.realtransform.ExplicitInvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.RealTransformSequence;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Scale;
import net.imglib2.realtransform.Scale2D;
import net.imglib2.realtransform.Scale3D;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.realtransform.Translation;
import net.imglib2.realtransform.Translation2D;
import net.imglib2.realtransform.Translation3D;
import net.imglib2.transform.integer.MixedTransform;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.MixedTransformView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;

/**
 * Class with helper methods for saving displacement field transformations as N5
 * datasets.
 *
 * @author John Bogovic
 *
 */
public class N5DisplacementField {

	public static final String MULTIPLIER_ATTR = "quantization_multiplier";
	public static final String AFFINE_ATTR = "affine";
	public static final String SPACING_ATTR = "spacing";
	public static final String OFFSET_ATTR = "offset";
	public static final String FORWARD_ATTR = "dfield";
	public static final String INVERSE_ATTR = "invdfield";

	public static final String EXTEND_ZERO = "ext_zero";
	public static final String EXTEND_MIRROR = "ext_mirror";
	public static final String EXTEND_BORDER = "ext_border";

	public final static int[] PERMUTATION2D = new int[]{2, 0, 1};
	public final static int[] PERMUTATION3D = new int[]{3, 0, 1, 2};

	/**
	 * Saves forward and inverse deformation fields into the default n5
	 * datasets.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5Writer
	 *            the n5 writer
	 * @param affine
	 *            the affine transform
	 * @param forwardDfield
	 *            the forward displacement field
	 * @param fwdspacing
	 *            the pixel spacing (resolution) of the forward deformation
	 *            field
	 * @param inverseDfield
	 *            the inverse displacement field
	 * @param invspacing
	 *            the pixel spacing (resolution) of the inverse deformation
	 *            field
	 * @param blockSize
	 *            the block size
	 * @param compression
	 *            the compression type
	 */
	public static <T extends NativeType<T> & RealType<T>> void save(
			final N5Writer n5Writer,
			final AffineGet affine,
			final RandomAccessibleInterval<T> forwardDfield,
			final double[] fwdspacing,
			final RandomAccessibleInterval<T> inverseDfield,
			final double[] invspacing,
			final int[] blockSize,
			final Compression compression) {

		save(n5Writer, FORWARD_ATTR, affine, forwardDfield, fwdspacing, blockSize, compression);
		save(n5Writer, INVERSE_ATTR, affine.inverse(), inverseDfield, invspacing, blockSize, compression);
	}

	/**
	 * Saves an affine transform and deformation field into a specified n5
	 * dataset.
	 *
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5Writer
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param affine
	 *            the affine transform
	 * @param dfield
	 *            the displacement field
	 * @param spacing
	 *            the pixel spacing (resolution) of the deformation field
	 * @param blockSize
	 *            the block size
	 * @param compression
	 *            the compression type
	 */
	public static <T extends NativeType<T> & RealType<T>> void save(
			final N5Writer n5Writer,
			final String dataset,
			final AffineGet affine,
			final RandomAccessibleInterval<T> dfield,
			final double[] spacing,
			final int[] blockSize,
			final Compression compression) {

		N5Utils.save(dfield, n5Writer, dataset, blockSize, compression);

		if (affine != null)
			saveAffine(affine, n5Writer, dataset);

		if (spacing != null)
			n5Writer.setAttribute(dataset, SPACING_ATTR, spacing);
	}

	/**
	 * Saves an affine transform and deformation field into a specified n5
	 * dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5Writer
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param affine
	 *            the affine transform
	 * @param dfield
	 *            the displacement field
	 * @param spacing
	 *            the pixel spacing (resolution) of the deformation field
	 * @param offset
	 *            the location of the origin of the deformation field.
	 * @param blockSize
	 *            the block size
	 * @param compression
	 *            the compression type
	 */
	public static <T extends NativeType<T> & RealType<T>> void save(
			final N5Writer n5Writer,
			final String dataset,
			final AffineGet affine,
			final RandomAccessibleInterval<T> dfield,
			final double[] spacing,
			final double[] offset,
			final int[] blockSize,
			final Compression compression) {

		N5Utils.save(dfield, n5Writer, dataset, blockSize, compression);

		if (affine != null)
			saveAffine(affine, n5Writer, dataset);

		if (spacing != null)
			n5Writer.setAttribute(dataset, SPACING_ATTR, spacing);

		if (offset != null)
			n5Writer.setAttribute(dataset, OFFSET_ATTR, offset);
	}

	/**
	 * Saves an affine transform and quantized deformation field into a
	 * specified n5 dataset.
	 *
	 * The deformation field here is saved as an {@link IntegerType} which could
	 * compress better in some cases. The multiplier from original values to
	 * compressed values is chosen as the smallest value that keeps the error
	 * (L2) between quantized and original vectors.
	 *
	 * @param <T>
	 *            the type parameter of the original displacement field
	 * @param <Q>
	 *            the type parameter of the quantized displacement field
	 * @param n5Writer
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param affine
	 *            the affine transform
	 * @param dfield
	 *            the displacement field
	 * @param spacing
	 *            the pixel spacing (resolution) of the deformation field
	 * @param blockSize
	 *            the block size
	 * @param compression
	 *            the compression type
	 * @param outputType
	 *            the type of the quantized output
	 * @param maxError
	 *            the desired maximum quantization error
	 */
	public static <T extends NativeType<T> & RealType<T>, Q extends NativeType<Q> & IntegerType<Q>> void save(
			final N5Writer n5Writer,
			final String dataset,
			final AffineGet affine,
			final RandomAccessibleInterval<T> dfield,
			final double[] spacing,
			final int[] blockSize,
			final Compression compression,
			final Q outputType,
			final double maxError) {

		saveQuantized(n5Writer, dataset, dfield, blockSize, compression, outputType, maxError);
		saveAffine(affine, n5Writer, dataset);
		if (spacing != null)
			n5Writer.setAttribute(dataset, SPACING_ATTR, spacing);
	}

	/**
	 * Saves an affine transform and quantized deformation field into a
	 * specified n5 dataset.
	 *
	 * The deformation field here is saved as an {@link IntegerType} which could
	 * compress better in some cases. The multiplier from original values to
	 * compressed values is chosen as the smallest value that keeps the error
	 * (L2) between quantized and original vectors.
	 *
	 * @param <T>
	 *            the type parameter of the original displacement field
	 * @param <Q>
	 *            the type parameter of the quantized displacement field
	 * @param n5Writer
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param affine
	 *            the affine transform
	 * @param dfield
	 *            the displacement field
	 * @param spacing
	 *            the pixel spacing (resolution) of the deformation field
	 * @param offset
	 *            the location of the origin of the deformation field.
	 * @param blockSize
	 *            the block size
	 * @param compression
	 *            the compression type
	 * @param outputType
	 *            the type of the quantized output
	 * @param maxError
	 *            the desired maximum quantization error
	 */
	public static <T extends NativeType<T> & RealType<T>, Q extends NativeType<Q> & IntegerType<Q>> void save(
			final N5Writer n5Writer,
			final String dataset,
			final AffineGet affine,
			final RandomAccessibleInterval<T> dfield,
			final double[] spacing,
			final double[] offset,
			final int[] blockSize,
			final Compression compression,
			final Q outputType,
			final double maxError) {

		saveQuantized(n5Writer, dataset, dfield, blockSize, compression, outputType, maxError);
		saveAffine(affine, n5Writer, dataset);
		if (spacing != null)
			n5Writer.setAttribute(dataset, SPACING_ATTR, spacing);
		if (offset != null)
			n5Writer.setAttribute(dataset, OFFSET_ATTR, offset);
	}

	/**
	 * Saves an affine transform as an attribute associated with an n5 dataset.
	 *
	 * @param affine
	 *            the affine transform
	 * @param n5Writer
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 */
	public static void saveAffine(
			final AffineGet affine,
			final N5Writer n5Writer,
			final String dataset) {

		if (affine != null)
			n5Writer.setAttribute(dataset, AFFINE_ATTR, affine.getRowPackedCopy());
	}

	/**
	 * Saves an affine transform and quantized deformation field into a
	 * specified n5 dataset.
	 *
	 * The deformation field here is saved as an {@link IntegerType} which could
	 * compress better in some cases. The multiplier from original values to
	 * compressed values is chosen as the smallest value that keeps the error
	 * (L2) between quantized and original vectors.
	 *
	 * @param <T>
	 *            the type parameter of the original displacement field
	 * @param <Q>
	 *            the type parameter of the quantized displacement field
	 * @param n5Writer
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param source
	 *            the source image
	 * @param blockSize
	 *            the block size
	 * @param compression
	 *            the compression type
	 * @param outputType
	 *            the type of the quantized output
	 * @param maxError
	 *            the desired maximum quantization error
	 */
	public static <T extends RealType<T>, Q extends NativeType<Q> & IntegerType<Q>> void saveQuantized(
			final N5Writer n5Writer,
			final String dataset,
			final RandomAccessibleInterval<T> source,
			final int[] blockSize,
			final Compression compression,
			final Q outputType,
			final double maxError) {

		/*
		 * To keep the max vector error below maxError, the error per coordinate
		 * must be below m
		 */
		final int nd = (source.numDimensions() - 1); // vector field source has
														// num
		// dims + 1
		final double m = 2 * Math.sqrt(maxError * maxError / nd);

		final RandomAccessibleInterval<T> source_permuted = vectorAxisFirst(source);
		final RandomAccessibleInterval<Q> source_quant = Converters.convert(
				source_permuted,
				new Converter<T, Q>() {

					@Override
					public void convert(final T input, final Q output) {

						output.setInteger(Math.round(input.getRealDouble() / m));
					}
				},
				outputType.copy());

		N5Utils.save(source_quant, n5Writer, dataset, blockSize, compression);
		n5Writer.setAttribute(dataset, MULTIPLIER_ATTR, m);
	}

	/**
	 * Opens an {@link InvertibleRealTransform} from an n5 object. Uses the
	 * provided datasets as the forward and inverse transformations.
	 *
	 * @param <T>
	 *            type parameter
	 * @param n5
	 *            the n5 reader
	 * @param forwardDataset
	 *            dataset path for the forward transform
	 * @param inverseDataset
	 *            dataset path for the inverse transform
	 * @param defaultType
	 *            the type
	 * @param interpolator
	 *            the interpolator factory
	 * @return the invertible transformation
	 */
	public static <T extends RealType<T> & NativeType<T>> ExplicitInvertibleRealTransform openInvertible(
			final N5Reader n5,
			final String forwardDataset,
			final String inverseDataset,
			final T defaultType,
			final InterpolatorFactory<RealComposite<T>, RandomAccessible<RealComposite<T>>> interpolator) {

		if (!n5.datasetExists(forwardDataset)) {
			System.err.println("dataset : " + forwardDataset + " does not exist.");
			return null;
		}

		if (!n5.datasetExists(inverseDataset)) {
			System.err.println("dataset : " + inverseDataset + " does not exist.");
			return null;
		}

		return new ExplicitInvertibleRealTransform(
				open(n5, forwardDataset, false, defaultType, interpolator),
				open(n5, inverseDataset, true, defaultType, interpolator));
	}

	/**
	 * Opens an {@link InvertibleRealTransform} from an n5 object using default
	 * datasets and linear interpolation.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @return the invertible transformation
	 */
	public static <T extends RealType<T> & NativeType<T>> ExplicitInvertibleRealTransform openInvertible(
			final N5Reader n5) {

		return openInvertible(n5, FORWARD_ATTR, INVERSE_ATTR, new DoubleType(), new NLinearInterpolatorFactory<>());
	}

	/**
	 * Opens an {@link InvertibleRealTransform} from an multi-scale n5 object,
	 * at the specified level, using linear interpolation.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param level
	 *            the scale level to open, if present
	 * @return the invertible transformation
	 */
	public static <T extends RealType<T> & NativeType<T>> ExplicitInvertibleRealTransform openInvertible(
			final N5Reader n5,
			final int level) {

		return openInvertible(n5, "/" + level + "/" + FORWARD_ATTR, "/" + level + "/" + INVERSE_ATTR, new DoubleType(), new NLinearInterpolatorFactory<>());
	}

	/**
	 * Opens an {@link InvertibleRealTransform} from an n5 object if possible.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param defaultType
	 *            the default type
	 * @param interpolator
	 *            the interpolator factory
	 * @return the invertible transformation
	 */
	public static <T extends RealType<T> & NativeType<T>> ExplicitInvertibleRealTransform openInvertible(
			final N5Reader n5,
			final T defaultType,
			final InterpolatorFactory<RealComposite<T>, RandomAccessible<RealComposite<T>>> interpolator) {

		return openInvertible(n5, FORWARD_ATTR, INVERSE_ATTR, defaultType, interpolator);
	}

	/**
	 * Opens a {@link RealTransform} from an n5 dataset as a displacement field.
	 * The resulting transform is the concatenation of an affine transform and a
	 * {@link DisplacementFieldTransform}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param inverse
	 *            whether to open the inverse transformation
	 * @param defaultType
	 *            the default type
	 * @param interpolator
	 *            the interpolator factory
	 * @return the transformation
	 */
	public static <T extends RealType<T> & NativeType<T>> RealTransform open(
			final N5Reader n5,
			final String dataset,
			final boolean inverse,
			final T defaultType,
			final InterpolatorFactory<RealComposite<T>, RandomAccessible<RealComposite<T>>> interpolator) {

		final AffineGet affine = openAffine(n5, dataset);
		final DisplacementFieldTransform dfield = new DisplacementFieldTransform(openCalibratedField(n5, dataset, interpolator, defaultType));
		if (affine != null) {
			final RealTransformSequence xfmSeq = new RealTransformSequence();
			if (inverse) {
				xfmSeq.add(affine);
				xfmSeq.add(dfield);
			} else {
				xfmSeq.add(dfield);
				xfmSeq.add(affine);
			}
			return xfmSeq;
		} else {
			return dfield;
		}
	}

	/**
	 * Returns an {@link AffineGet} transform from pixel space to physical
	 * space, for the given n5 dataset, if present, null otherwise.
	 *
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @return the affine transform
	 */
	public static AffineGet openPixelToPhysical(final N5Reader n5, final String dataset) {

		final double[] spacing = n5.getAttribute(dataset, SPACING_ATTR, double[].class);
		final double[] offset = n5.getAttribute(dataset, OFFSET_ATTR, double[].class);
		if (spacing == null && offset == null) {
			return null;
		} else if (offset == null) {
			final int N = spacing.length;
			if (N == 2)
				return new Scale2D(spacing);
			else if (N == 3)
				return new Scale3D(spacing);
			else
				return new Scale(spacing);
		} else if (spacing == null) {
			final int N = offset.length;
			if (N == 2)
				return new Translation2D(offset);
			else if (N == 3)
				return new Translation3D(offset);
			else
				return new Translation(offset);
		} else {
			return new ScaleAndTranslation(spacing, offset);
		}
	}

	/**
	 * Returns and affine transform stored as an attribute in an n5 dataset.
	 *
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @return the affine transformation
	 */
	public static AffineGet openAffine(final N5Reader n5, final String dataset) {

		final double[] affineMtxRow = n5.getAttribute(dataset, AFFINE_ATTR, double[].class);
		if (affineMtxRow == null)
			return null;

		final int N = affineMtxRow.length;
		final AffineTransform affineMtx;
		if (N == 2)
			affineMtx = new AffineTransform(1);
		else if (N == 6)
			affineMtx = new AffineTransform(2);
		else if (N == 12)
			affineMtx = new AffineTransform(3);
		else
			return null;

		affineMtx.set(affineMtxRow);
		return affineMtx;
	}

	/**
	 * Returns a deformation field from the given n5 dataset.
	 *
	 * If the data is an {@link IntegerType}, returns an un-quantized view of
	 * the dataset, otherwise, returns the raw {@link RandomAccessibleInterval}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param <Q>
	 *            the quantized type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultType
	 *            the default type
	 * @return the deformation field as a RandomAccessibleInterval
	 */
	@SuppressWarnings("unchecked")
	public static <T extends NativeType<T> & RealType<T>, Q extends NativeType<Q> & IntegerType<Q>> RandomAccessibleInterval<T> openField(
			final N5Reader n5,
			final String dataset,
			final T defaultType) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		switch (attributes.getDataType()) {
		case INT8:
			return openQuantized(n5, dataset, (Q)new ByteType(), defaultType);
		case UINT8:
			return openQuantized(n5, dataset, (Q)new UnsignedByteType(), defaultType);
		case INT16:
			return openQuantized(n5, dataset, (Q)new ShortType(), defaultType);
		case UINT16:
			return openQuantized(n5, dataset, (Q)new UnsignedShortType(), defaultType);
		case INT32:
			return openQuantized(n5, dataset, (Q)new IntType(), defaultType);
		case UINT32:
			return openQuantized(n5, dataset, (Q)new UnsignedIntType(), defaultType);
		case INT64:
			return openQuantized(n5, dataset, (Q)new LongType(), defaultType);
		case UINT64:
			return openQuantized(n5, dataset, (Q)new UnsignedLongType(), defaultType);
		default:
			return openRaw(n5, dataset, defaultType);
		}
	}

	/**
	 * Returns a deformation field in physical coordinates as a
	 * {@link RealRandomAccessible} from an n5 dataset.
	 *
	 * Internally, opens the given n5 dataset as a
	 * {@link RandomAccessibleInterval}, un-quantizes if necessary, uses the
	 * input {@link InterpolatorFactory} for interpolation, and transforms to
	 * physical coordinates using the pixel spacing stored in the "spacing"
	 * attribute, if present.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param interpolator
	 *            the interpolato factory
	 * @param defaultType
	 *            the default type
	 * @return the deformation field as a RealRandomAccessible
	 */
	public static <T extends NativeType<T> & RealType<T>> RealRandomAccessible<RealComposite<T>> openCalibratedField(
			final N5Reader n5, final String dataset,
			final InterpolatorFactory<RealComposite<T>, RandomAccessible<RealComposite<T>>> interpolator,
			final T defaultType) {

		return openCalibratedField(n5, dataset, interpolator, EXTEND_BORDER, defaultType);
	}

	/**
	 * Returns coordinate displacements in physical coordinates as a
	 * {@link RealRandomAccessible} from an n5 dataset.
	 *
	 * Internally, opens the given n5 dataset as a
	 * {@link RandomAccessibleInterval}, un-quantizes if necessary, uses the
	 * input {@link InterpolatorFactory} for interpolation, and transforms to
	 * physical coordinates using the pixel spacing stored in the "spacing"
	 * attribute, if present.
	 *
	 * The ith {@link RealRandomAccessible} contains the displacements for the
	 * ith coordinate. The output of this method can be passed directly to the
	 * constructor of {@link DisplacementFieldTransform}.
	 *
	 * @param <T>
	 *            the type parameter for the underlying data
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the n5 dataset
	 * @param interpolator
	 *            the type of interpolation
	 * @param extensionType
	 *            the type of out-of-bounds extension
	 * @param defaultType
	 *            the type
	 * @return the coordinate displacements
	 */
	public static <T extends NativeType<T> & RealType<T>> RealRandomAccessible<RealComposite<T>> openCalibratedField(
			final N5Reader n5,
			final String dataset,
			final InterpolatorFactory<RealComposite<T>, RandomAccessible<RealComposite<T>>> interpolator,
			final String extensionType,
			final T defaultType) {

		final RandomAccessibleInterval<T> dfieldRai = openField(n5, dataset, defaultType);
		if (dfieldRai == null) {
			return null;
		}

		final CompositeIntervalView<T, RealComposite<T>> dfieldRaiPerm = Views.collapseReal(vectorAxisLast(dfieldRai));
		final RealRandomAccessible<RealComposite<T>> dfieldReal;
		if (extensionType.equals(EXTEND_MIRROR)) {
			dfieldReal = Views.interpolate(Views.extendMirrorDouble(dfieldRaiPerm), interpolator);
		} else if (extensionType.equals(EXTEND_BORDER)) {
			dfieldReal = Views.interpolate(Views.extendBorder(dfieldRaiPerm), interpolator);
		} else {
			dfieldReal = Views.interpolate(Views.extendZero(dfieldRaiPerm), interpolator);
		}

		final RealRandomAccessible<RealComposite<T>> displacements;
		final AffineGet pix2Phys = openPixelToPhysical(n5, dataset);
		if (pix2Phys != null)
			displacements = RealViews.affine(dfieldReal, pix2Phys);
		else
			displacements = dfieldReal;

		return displacements;
	}

	/**
	 * Opens a transform from an n5 dataset using linear interpolation for the
	 * deformation field.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param inverse
	 *            whether to open the inverse
	 * @return the transform
	 */
	public static <T extends NativeType<T> & RealType<T>> RealTransform open(
			final N5Reader n5, final String dataset, final boolean inverse) {

		return open(n5, dataset, inverse, new FloatType(), new NLinearInterpolatorFactory<RealComposite<FloatType>>());
	}

	/**
	 * Returns a deformation field as a {@link RandomAccessibleInterval},
	 * ensuring that the vector is stored in the last dimension.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultType
	 *            the default type
	 * @return the deformation field
	 */
	public static <T extends RealType<T> & NativeType<T>> RandomAccessibleInterval<T> openRaw(
			final N5Reader n5,
			final String dataset,
			final T defaultType) {

		final RandomAccessibleInterval<T> src = N5Utils.open(n5, dataset, defaultType);
		return vectorAxisLast(src);
	}

	/**
	 * Open a quantized (integer) {@link RandomAccessibleInterval} from an n5
	 * dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param <Q>
	 *            the quantized type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultQuantizedType
	 *            the quantized type
	 * @param defaultType
	 *            the original type
	 * @return the un-quantized data
	 */
	public static <Q extends RealType<Q> & NativeType<Q>, T extends RealType<T>> RandomAccessibleInterval<T> openQuantized(
			final N5Reader n5,
			final String dataset,
			final Q defaultQuantizedType,
			final T defaultType) {

		final RandomAccessibleInterval<Q> src = N5Utils.open(n5, dataset, defaultQuantizedType);

		// get the factor going from quantized to original values
		final Double mattr = n5.getAttribute(dataset, MULTIPLIER_ATTR, Double.TYPE);
		final double m;
		if (mattr != null)
			m = mattr.doubleValue();
		else
			m = 1.0;

		final RandomAccessibleInterval<Q> src_perm = vectorAxisLast(src);
		final RandomAccessibleInterval<T> src_converted = Converters.convert(
				src_perm,
				new Converter<Q, T>() {

					@Override
					public void convert(final Q input, final T output) {

						output.setReal(input.getRealDouble() * m);
					}
				},
				defaultType.copy());

		return src_converted;
	}

	/**
	 * Returns a deformation field as a {@link RandomAccessibleInterval} with
	 * the vector stored in the last dimension.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source displacement field
	 * @return the possibly permuted deformation field
	 */
	public static <T extends RealType<T>> RandomAccessibleInterval<T> vectorAxisLast(final RandomAccessibleInterval<T> source) {

		final int n = source.numDimensions();

		if (source.dimension(n - 1) == (n - 1))
			return source;
		else if (source.dimension(0) == (n - 1)) {
			final int[] component = new int[n];
			component[0] = n - 1;
			for (int i = 1; i < n; ++i)
				component[i] = i - 1;

			return permute(source, component);
		}

		throw new RuntimeException(
				String.format("Displacement fields must store vector components in the first or last dimension. " +
						"Found a %d-d volume; expect size [%d,...] or [...,%d]", n, (n - 1), (n - 1)));
	}

	/**
	 * Returns a deformation field as a {@link RandomAccessibleInterval} with
	 * the vector stored in the first dimension.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source displacement field
	 * @return the possibly permuted deformation field
	 */
	public static <T extends RealType<T>> RandomAccessibleInterval<T> vectorAxisFirst(final RandomAccessibleInterval<T> source) {

		final int n = source.numDimensions();

		if (source.dimension(0) == (n - 1))
			return source;
		else if (source.dimension(n - 1) == (n - 1)) {
			final int[] component = new int[n];
			component[n - 1] = 0;
			for (int i = 0; i < n - 1; ++i)
				component[i] = i + 1;

			return permute(source, component);
		}

		throw new RuntimeException(
				String.format("Displacement fields must store vector components in the first or last dimension. " +
						"Found a %d-d volume; expect size [%d,...] or [...,%d]", n, (n - 1), (n - 1)));
	}

	/**
	 * Permutes the dimensions of a {@link RandomAccessibleInterval} using the
	 * given permutation vector, where the ith value in p gives destination of
	 * the ith input dimension in the output.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source data
	 * @param p
	 *            the permutation
	 * @return the permuted source
	 */
	public static <T> IntervalView<T> permute(final RandomAccessibleInterval<T> source, final int[] p) {

		final int n = source.numDimensions();

		final long[] min = new long[n];
		final long[] max = new long[n];
		for (int i = 0; i < n; ++i) {
			min[p[i]] = source.min(i);
			max[p[i]] = source.max(i);
		}

		final MixedTransform t = new MixedTransform(n, n);
		t.setComponentMapping(p);

		return Views.interval(new MixedTransformView<T>(source, t), min, max);
	}

	/**
	 * Returns a two element double array in which the first and second elements
	 * store the minimum and maximum values of the input
	 * {@link IterableInterval}, respectively.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param img
	 *            the iterable interval
	 * @return the min and max values stored in a double array
	 */
	public static <T extends RealType<T>> double[] getMinMax(final IterableInterval<T> img) {

		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;
		final Cursor<T> c = img.cursor();
		while (c.hasNext()) {
			final double v = Math.abs(c.next().getRealDouble());
			if (v > max)
				max = v;

			if (v < min)
				min = v;
		}
		return new double[]{min, max};
	}
}
