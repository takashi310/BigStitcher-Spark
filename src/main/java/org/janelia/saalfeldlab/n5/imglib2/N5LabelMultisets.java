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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.LoaderCache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.cache.util.LoaderCacheAsCacheAdapter;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.type.label.Label;
import net.imglib2.type.label.LabelMultisetType;
import net.imglib2.type.label.LabelMultisetType.Entry;
import net.imglib2.type.label.LabelUtils;
import net.imglib2.type.label.VolatileLabelMultisetArray;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class N5LabelMultisets {

	public static final String LABEL_MULTISETTYPE_KEY = "isLabelMultiset";

	/**
	 * Determine whether an N5 dataset is of type {@link LabelMultisetType}.
	 *
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @return true of the dataset is of type LabelMultiset
	 */
	public static boolean isLabelMultisetType(final N5Reader n5, final String dataset) {

		return Optional
				.ofNullable(n5.getAttribute(dataset, LABEL_MULTISETTYPE_KEY, Boolean.class))
				.orElse(false);
	}

	/**
	 * Open an N5 dataset of {@link LabelMultisetType} as a memory cached
	 * {@link LazyCellImg}.
	 *
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @return the LabelMultiset image
	 */
	public static CachedCellImg<LabelMultisetType, VolatileLabelMultisetArray> openLabelMultiset(
			final N5Reader n5,
			final String dataset) {

		return openLabelMultiset(n5, dataset, Label.BACKGROUND);
	}

	/**
	 * Open an N5 dataset of {@link LabelMultisetType} as a memory cached
	 * {@link LazyCellImg}.
	 *
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultLabelId
	 *            the default label
	 * @return the LabelMultiset image
	 */
	public static CachedCellImg<LabelMultisetType, VolatileLabelMultisetArray> openLabelMultiset(
			final N5Reader n5,
			final String dataset,
			final long defaultLabelId) {

		return openLabelMultiset(n5, dataset, N5LabelMultisetCacheLoader.constantNullReplacement(defaultLabelId));
	}

	/**
	 * Open an N5 dataset of {@link LabelMultisetType} as a memory cached
	 * {@link LazyCellImg}.
	 *
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param nullReplacement
	 *            a function returning data for null blocks
	 * @return the LabelMultiset image
	 */
	public static CachedCellImg<LabelMultisetType, VolatileLabelMultisetArray> openLabelMultiset(
			final N5Reader n5,
			final String dataset,
			final BiFunction<CellGrid, long[], byte[]> nullReplacement) {

		return openLabelMultiset(n5, dataset, nullReplacement, new SoftRefLoaderCache<>());
	}

	/**
	 * Open an N5 dataset of {@link LabelMultisetType} as a memory cached
	 * {@link LazyCellImg}.
	 *
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param nullReplacement
	 *            a function returning data for null blocks
	 * @param loaderCache
	 *            the cache
	 * @return the LabelMultiset image
	 */
	public static CachedCellImg<LabelMultisetType, VolatileLabelMultisetArray> openLabelMultiset(
			final N5Reader n5,
			final String dataset,
			final BiFunction<CellGrid, long[], byte[]> nullReplacement,
			final LoaderCache<Long, Cell<VolatileLabelMultisetArray>> loaderCache) {

		if (!isLabelMultisetType(n5, dataset))
			throw new N5IOException(dataset + " is not a label multiset dataset.");

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		final CellGrid grid = new CellGrid(attributes.getDimensions(), attributes.getBlockSize());

		final N5LabelMultisetCacheLoader loader = new N5LabelMultisetCacheLoader(n5, dataset, nullReplacement);
		final LoaderCacheAsCacheAdapter<Long, Cell<VolatileLabelMultisetArray>> wrappedCache = new LoaderCacheAsCacheAdapter<>(loaderCache, loader);

		final CachedCellImg<LabelMultisetType, VolatileLabelMultisetArray> cachedImg = new CachedCellImg<>(
				grid,
				new LabelMultisetType().getEntitiesPerPixel(),
				wrappedCache,
				new VolatileLabelMultisetArray(0, true, new long[]{Label.INVALID}));
		cachedImg.setLinkedType(new LabelMultisetType(cachedImg));
		return cachedImg;
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * as an N5 dataset.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param blockSize
	 *            block size
	 * @param compression
	 *            compression type
	 */
	public static void saveLabelMultiset(
			RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression) {

		source = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(source);
		final DatasetAttributes attributes = new DatasetAttributes(
				dimensions,
				blockSize,
				DataType.UINT8,
				compression);

		n5.createDataset(dataset, attributes);
		n5.setAttribute(dataset, LABEL_MULTISETTYPE_KEY, true);

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray(source);
		final long[] offset = new long[n];
		final long[] gridPosition = new long[n];
		final int[] intCroppedBlockSize = new int[n];
		final long[] longCroppedBlockSize = new long[n];
		for (int d = 0; d < n;) {
			cropBlockDimensions(max, offset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition);
			final RandomAccessibleInterval<LabelMultisetType> sourceBlock = Views.offsetInterval(source, offset, longCroppedBlockSize);
			final ByteArrayDataBlock dataBlock = createDataBlock(sourceBlock, gridPosition);

			n5.writeBlock(dataset, attributes, dataBlock);

			for (d = 0; d < n; ++d) {
				offset[d] += blockSize[d];
				if (offset[d] <= max[d])
					break;
				else
					offset[d] = 0;
			}
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * as an N5 dataset, multi-threaded.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param blockSize
	 *            the block size
	 * @param compression
	 *            the compression type
	 * @param exec
	 *            the executor service
	 * @throws InterruptedException
	 *             interrupted exception
	 * @throws ExecutionException
	 *             execution exception
	 */
	public static void saveLabelMultiset(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		final RandomAccessibleInterval<LabelMultisetType> zeroMinSource = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(zeroMinSource);
		final DatasetAttributes attributes = new DatasetAttributes(
				dimensions,
				blockSize,
				DataType.UINT8,
				compression);

		n5.createDataset(dataset, attributes);
		n5.setAttribute(dataset, LABEL_MULTISETTYPE_KEY, true);

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray(zeroMinSource);
		final long[] offset = new long[n];

		final ArrayList<Future<?>> futures = new ArrayList<>();
		for (int d = 0; d < n;) {
			final long[] fOffset = offset.clone();

			futures.add(
					exec.submit(
							() -> {

								final long[] gridPosition = new long[n];
								final int[] intCroppedBlockSize = new int[n];
								final long[] longCroppedBlockSize = new long[n];

								cropBlockDimensions(
										max,
										fOffset,
										blockSize,
										longCroppedBlockSize,
										intCroppedBlockSize,
										gridPosition);

								final RandomAccessibleInterval<LabelMultisetType> sourceBlock = Views
										.offsetInterval(zeroMinSource, fOffset, longCroppedBlockSize);
								final ByteArrayDataBlock dataBlock = createDataBlock(sourceBlock, gridPosition);
								n5.writeBlock(dataset, attributes, dataBlock);
							}));

			for (d = 0; d < n; ++d) {
				offset[d] += blockSize[d];
				if (offset[d] <= max[d])
					break;
				else
					offset[d] = 0;
			}
		}
		for (final Future<?> f : futures)
			f.get();
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * into an existing N5 dataset.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            dataset attributes
	 * @param gridOffset
	 *            the offset of this block in grid coordinates
	 */
	public static void saveLabelMultisetBlock(
			RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset) {

		if (!isLabelMultisetType(n5, dataset))
			throw new N5IOException(dataset + " is not a label multiset dataset.");

		source = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(source);

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray(source);
		final long[] offset = new long[n];
		final long[] gridPosition = new long[n];
		final int[] blockSize = attributes.getBlockSize();
		final int[] intCroppedBlockSize = new int[n];
		final long[] longCroppedBlockSize = new long[n];
		for (int d = 0; d < n;) {
			cropBlockDimensions(
					max,
					offset,
					gridOffset,
					blockSize,
					longCroppedBlockSize,
					intCroppedBlockSize,
					gridPosition);
			final RandomAccessibleInterval<LabelMultisetType> sourceBlock = Views.offsetInterval(source, offset, longCroppedBlockSize);
			final ByteArrayDataBlock dataBlock = createDataBlock(sourceBlock, gridPosition);

			n5.writeBlock(dataset, attributes, dataBlock);

			for (d = 0; d < n; ++d) {
				offset[d] += blockSize[d];
				if (offset[d] <= max[d])
					break;
				else
					offset[d] = 0;
			}
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * into an existing N5 dataset. The block offset is determined by the source
	 * position, and the source is assumed to align with the {@link DataBlock}
	 * grid of the dataset.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 */
	public static void saveLabelMultisetBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes) {

		final int[] blockSize = attributes.getBlockSize();
		final long[] gridOffset = new long[blockSize.length];
		Arrays.setAll(gridOffset, d -> source.min(d) / blockSize[d]);
		saveLabelMultisetBlock(source, n5, dataset, attributes, gridOffset);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * into an existing N5 dataset. The block offset is determined by the source
	 * position, and the source is assumed to align with the {@link DataBlock}
	 * grid of the dataset.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 */
	public static void saveLabelMultisetBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveLabelMultisetBlock(source, n5, dataset, attributes);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * into an existing N5 dataset.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the offset of the block in grid coordinates
	 */
	public static void saveLabelMultisetBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveLabelMultisetBlock(source, n5, dataset, attributes, gridOffset);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * into an existing N5 dataset, multi-threaded.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the offset of the block in grid coordinates
	 * @param exec
	 *            the executor service
	 * @throws InterruptedException
	 *             interrupted exception
	 * @throws ExecutionException
	 *             execution exception
	 */
	public static void saveLabelMultisetBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		if (!isLabelMultisetType(n5, dataset))
			throw new N5IOException(dataset + " is not a label multiset dataset.");

		final RandomAccessibleInterval<LabelMultisetType> zeroMinSource = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(zeroMinSource);
		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			final int n = dimensions.length;
			final long[] max = Intervals.maxAsLongArray(zeroMinSource);
			final long[] offset = new long[n];
			final int[] blockSize = attributes.getBlockSize();

			final ArrayList<Future<?>> futures = new ArrayList<>();
			for (int d = 0; d < n;) {
				final long[] fOffset = offset.clone();

				futures.add(
						exec.submit(
								() -> {

									final long[] gridPosition = new long[n];
									final int[] intCroppedBlockSize = new int[n];
									final long[] longCroppedBlockSize = new long[n];

									cropBlockDimensions(
											max,
											fOffset,
											gridOffset,
											blockSize,
											longCroppedBlockSize,
											intCroppedBlockSize,
											gridPosition);

									final RandomAccessibleInterval<LabelMultisetType> sourceBlock = Views
											.offsetInterval(zeroMinSource, fOffset, longCroppedBlockSize);
									final ByteArrayDataBlock dataBlock = createDataBlock(sourceBlock, gridPosition);

									n5.writeBlock(dataset, attributes, dataBlock);
								}));

				for (d = 0; d < n; ++d) {
					offset[d] += blockSize[d];
					if (offset[d] <= max[d])
						break;
					else
						offset[d] = 0;
				}
			}
			for (final Future<?> f : futures)
				f.get();
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * into an N5 dataset at a given offset. The offset is given in
	 * {@link DataBlock} grid coordinates and the source is assumed to align
	 * with the {@link DataBlock} grid of the dataset. Only {@link DataBlock
	 * DataBlocks} that contain labels other than a given default label are
	 * stored.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 * @param gridOffset
	 *            the offset of the block in grid coordinates
	 * @param defaultLabelId
	 *            the default label
	 */
	public static void saveLabelMultisetNonEmptyBlock(
			RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset,
			final long defaultLabelId) {

		if (!isLabelMultisetType(n5, dataset))
			throw new N5IOException(dataset + " is not a label multiset dataset.");

		source = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(source);

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray(source);
		final long[] offset = new long[n];
		final long[] gridPosition = new long[n];
		final int[] blockSize = attributes.getBlockSize();
		final int[] intCroppedBlockSize = new int[n];
		final long[] longCroppedBlockSize = new long[n];
		for (int d = 0; d < n;) {
			cropBlockDimensions(
					max,
					offset,
					gridOffset,
					blockSize,
					longCroppedBlockSize,
					intCroppedBlockSize,
					gridPosition);
			final RandomAccessibleInterval<LabelMultisetType> sourceBlock = Views.offsetInterval(source, offset, longCroppedBlockSize);
			final ByteArrayDataBlock dataBlock = createNonEmptyDataBlock(sourceBlock, gridPosition, defaultLabelId);

			if (dataBlock != null)
				n5.writeBlock(dataset, attributes, dataBlock);

			for (d = 0; d < n; ++d) {
				offset[d] += blockSize[d];
				if (offset[d] <= max[d])
					break;
				else
					offset[d] = 0;
			}
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * into an N5 dataset. The block offset is determined by the source
	 * position, and the source is assumed to align with the {@link DataBlock}
	 * grid of the dataset. Only {@link DataBlock DataBlocks} that contain
	 * labels other than a given default label are stored.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 * @param defaultLabelId
	 *            the default label
	 */
	public static void saveLabelMultisetNonEmptyBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long defaultLabelId) {

		final int[] blockSize = attributes.getBlockSize();
		final long[] gridOffset = new long[blockSize.length];
		Arrays.setAll(gridOffset, d -> source.min(d) / blockSize[d]);
		saveLabelMultisetNonEmptyBlock(source, n5, dataset, attributes, gridOffset, defaultLabelId);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * into an N5 dataset. The block offset is determined by the source
	 * position, and the source is assumed to align with the {@link DataBlock}
	 * grid of the dataset. Only {@link DataBlock DataBlocks} that contain
	 * labels other than a given default label are stored.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param defaultLabelId
	 *            the default label
	 */
	public static void saveLabelMultisetNonEmptyBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final long defaultLabelId) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveLabelMultisetNonEmptyBlock(source, n5, dataset, attributes, defaultLabelId);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * into an N5 dataset. The block offset is determined by the source
	 * position, and the source is assumed to align with the {@link DataBlock}
	 * grid of the dataset. Only {@link DataBlock DataBlocks} that contain
	 * labels other than {@link Label#BACKGROUND} are stored.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 */
	public static void saveLabelMultisetNonEmptyBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset) {

		saveLabelMultisetNonEmptyBlock(source, n5, dataset, Label.BACKGROUND);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * into an N5 dataset at a given offset. The offset is given in
	 * {@link DataBlock} grid coordinates and the source is assumed to align
	 * with the {@link DataBlock} grid of the dataset. Only {@link DataBlock
	 * DataBlocks} that contain labels other than a given default label are
	 * stored.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the offset of the block in grid coordinates
	 * @param defaultLabelId
	 *            the default label
	 */
	public static void saveLabelMultisetNonEmptyBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final long defaultLabelId) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveLabelMultisetNonEmptyBlock(source, n5, dataset, attributes, gridOffset, defaultLabelId);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType}
	 * into an N5 dataset at a given offset. The offset is given in
	 * {@link DataBlock} grid coordinates and the source is assumed to align
	 * with the {@link DataBlock} grid of the dataset. Only {@link DataBlock
	 * DataBlocks} that contain labels other than {@link Label#BACKGROUND} are
	 * stored.
	 *
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the offset of the block in grid coordinates
	 */
	public static void saveLabelMultisetNonEmptyBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset) {

		saveLabelMultisetNonEmptyBlock(source, n5, dataset, gridOffset, Label.BACKGROUND);
	}

	/**
	 * Creates a {@link ByteArrayDataBlock} with serialized source contents of
	 * type {@link LabelMultisetType}.
	 *
	 * @param source
	 *            the source image
	 * @param gridPosition
	 *            the position of the block
	 * @return the data block
	 */
	private static ByteArrayDataBlock createDataBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final long[] gridPosition) {

		final byte[] data = LabelUtils.serializeLabelMultisetTypes(
				Views.flatIterable(source),
				(int)Intervals.numElements(source));

		final ByteArrayDataBlock dataBlock = new ByteArrayDataBlock(
				Intervals.dimensionsAsIntArray(source),
				gridPosition,
				data);

		return dataBlock;
	}

	/**
	 * Creates a {@link ByteArrayDataBlock} with serialized source contents of
	 * type {@link LabelMultisetType}, or returns {@code null} if all labels are
	 * equal to {@code defaultLabelId} (regardless of their counts).
	 *
	 * @param source
	 *            the source image
	 * @param gridPosition
	 *            the position of the block
	 * @param defaultLabelId
	 *            the default label
	 * @return the data block
	 */
	private static ByteArrayDataBlock createNonEmptyDataBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final long[] gridPosition,
			final long defaultLabelId) {

		boolean isEmpty = true;
		for (final LabelMultisetType lmt : Views.iterable(source))
			for (final Entry<Label> entry : lmt.entrySet())
				isEmpty &= entry.getElement().id() == defaultLabelId;

		return isEmpty ? null : createDataBlock(source, gridPosition);
	}

	static void cropBlockDimensions(
			final long[] max,
			final long[] offset,
			final long[] gridOffset,
			final int[] blockDimensions,
			final long[] croppedBlockDimensions,
			final int[] intCroppedBlockDimensions,
			final long[] gridPosition) {

		for (int d = 0; d < max.length; ++d) {
			croppedBlockDimensions[d] = Math.min(blockDimensions[d], max[d] - offset[d] + 1);
			intCroppedBlockDimensions[d] = (int)croppedBlockDimensions[d];
			gridPosition[d] = offset[d] / blockDimensions[d] + gridOffset[d];
		}
	}

	static void cropBlockDimensions(
			final long[] max,
			final long[] offset,
			final int[] blockDimensions,
			final long[] croppedBlockDimensions,
			final int[] intCroppedBlockDimensions,
			final long[] gridPosition) {

		for (int d = 0; d < max.length; ++d) {
			croppedBlockDimensions[d] = Math.min(blockDimensions[d], max[d] - offset[d] + 1);
			intCroppedBlockDimensions[d] = (int)croppedBlockDimensions[d];
			gridPosition[d] = offset[d] / blockDimensions[d];
		}
	}
}
