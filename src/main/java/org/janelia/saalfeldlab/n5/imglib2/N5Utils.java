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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import java.util.stream.Collectors;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.LocalizableSampler;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.blocks.PrimitiveBlocks;
import net.imglib2.blocks.PrimitiveBlocks.OnFallback;
import net.imglib2.blocks.SubArrayCopy;
import net.imglib2.blocks.TempArray;
import net.imglib2.cache.Cache;
import net.imglib2.cache.CacheLoader;
import net.imglib2.cache.LoaderCache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.DiskCachedCellImgFactory;
import net.imglib2.cache.img.DiskCachedCellImgOptions;
import net.imglib2.cache.ref.BoundedSoftRefLoaderCache;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.img.basictypeaccess.ArrayDataAccessFactory;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.img.basictypeaccess.volatiles.VolatileAccess;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.stream.Streams;
import net.imglib2.type.NativeType;
import net.imglib2.type.PrimitiveType;
import net.imglib2.type.label.LabelMultisetType;
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
import net.imglib2.util.Cast;
import net.imglib2.util.CloseableThreadLocal;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

import org.janelia.saalfeldlab.n5.imglib2.N5BlockValidateAndRetry;

/**
 * Static utility methods to open N5 datasets as ImgLib2
 * {@link RandomAccessibleInterval RandomAccessibleIntervals} and to save
 * ImgLib2 {@link RandomAccessibleInterval RandomAccessibleIntervals} as
 * [sparse] N5 datasets.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 * @author John Bogovic &lt;bogovicj@janelia.hhmi.org&gt;
 */
public class N5Utils {

	public static int RETRY_NUM = 3;
	public static int WAIT_TIME = 5;

	private N5Utils() {}

	public static <T extends NativeType<T>> DataType dataType(final T type) {

		if (DoubleType.class.isInstance(type))
			return DataType.FLOAT64;
		if (FloatType.class.isInstance(type))
			return DataType.FLOAT32;
		if (LongType.class.isInstance(type))
			return DataType.INT64;
		if (UnsignedLongType.class.isInstance(type))
			return DataType.UINT64;
		if (IntType.class.isInstance(type))
			return DataType.INT32;
		if (UnsignedIntType.class.isInstance(type))
			return DataType.UINT32;
		if (ShortType.class.isInstance(type))
			return DataType.INT16;
		if (UnsignedShortType.class.isInstance(type))
			return DataType.UINT16;
		if (ByteType.class.isInstance(type))
			return DataType.INT8;
		if (UnsignedByteType.class.isInstance(type))
			return DataType.UINT8;
		else
			return null;
	}

	@SuppressWarnings("unchecked")
	public static <T extends NativeType<T>> T type(final DataType dataType) {

		switch (dataType) {
		case INT8:
			return (T)new ByteType();
		case UINT8:
			return (T)new UnsignedByteType();
		case INT16:
			return (T)new ShortType();
		case UINT16:
			return (T)new UnsignedShortType();
		case INT32:
			return (T)new IntType();
		case UINT32:
			return (T)new UnsignedIntType();
		case INT64:
			return (T)new LongType();
		case UINT64:
			return (T)new UnsignedLongType();
		case FLOAT32:
			return (T)new FloatType();
		case FLOAT64:
			return (T)new DoubleType();
		default:
			return null;
		}
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}. Supports all
	 * primitive types and {@link LabelMultisetType}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @return the image
	 */
	@SuppressWarnings("unchecked")
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset) {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset))
			return (CachedCellImg<T, ?>)N5LabelMultisets.openLabelMultiset(n5, dataset);
		else
			return open(n5, dataset, (Consumer<IterableInterval<T>>)img -> {});
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param maxNumCacheEntries
	 *            the max number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries) {

		return openWithBoundedSoftRefCache(n5, dataset, (Consumer<IterableInterval<T>>)img -> {}, maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}. Supports all primitive types and
	 * {@link LabelMultisetType}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @return the image
	 */
	@SuppressWarnings("unchecked")
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset) {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset))
			return (CachedCellImg<T, ?>)N5LabelMultisets.openLabelMultiset(n5, dataset);
		else
			return openVolatile(n5, dataset, (Consumer<IterableInterval<T>>)img -> {});
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param maxNumCacheEntries
	 *            the max number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries) {

		return openVolatileWithBoundedSoftRefCache(n5, dataset, (Consumer<IterableInterval<T>>)img -> {}, maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a disk-cached {@link LazyCellImg}. Note that this
	 * requires that all parts of the the N5 dataset that will be accessed fit
	 * into /tmp.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithDiskCache(
			final N5Reader n5,
			final String dataset) {

		return openWithDiskCache(n5, dataset, (Consumer<IterableInterval<T>>)img -> {});
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) {

		return open(n5, dataset, N5CacheLoader.setToDefaultValue(defaultValue));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 * @param maxNumCacheEntries
	 *            the max number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries,
			final T defaultValue) {

		return openWithBoundedSoftRefCache(n5, dataset, N5CacheLoader.setToDefaultValue(defaultValue), maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) {

		return openVolatile(n5, dataset, N5CacheLoader.setToDefaultValue(defaultValue));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 * @param maxNumCacheEntries
	 *            the maximum number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries,
			final T defaultValue) {

		return openVolatileWithBoundedSoftRefCache(n5, dataset, N5CacheLoader.setToDefaultValue(defaultValue), maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a disk-cached {@link LazyCellImg}. Note that this
	 * requires that all parts of the the N5 dataset that will be accessed fit
	 * into /tmp.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithDiskCache(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) {

		return openWithDiskCache(n5, dataset, N5CacheLoader.setToDefaultValue(defaultValue));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) {

		return open(n5, dataset, blockNotFoundHandler, AccessFlags.setOf());
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @param accessFlags
	 *            the access flag set
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final Set<AccessFlags> accessFlags) {

		return open(n5, dataset, blockNotFoundHandler, dataType -> new SoftRefLoaderCache<>(), accessFlags);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on
	 * the number of cache entries.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @param maxNumCacheEntries
	 *            the maximum number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries) {

		return openWithBoundedSoftRefCache(n5, dataset, blockNotFoundHandler, maxNumCacheEntries, AccessFlags.setOf());
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on
	 * the number of cache entries.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @param maxNumCacheEntries
	 *            the maximum number of cache entries
	 * @param accessFlags
	 *            the access flag set
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries,
			final Set<AccessFlags> accessFlags) {

		return open(n5, dataset, blockNotFoundHandler, dataType -> new BoundedSoftRefLoaderCache<>(maxNumCacheEntries), accessFlags);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @param loaderCacheFactory
	 *            the cache factory
	 * @param accessFlags
	 *            the access flag set
	 * @return the image
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final Function<DataType, LoaderCache> loaderCacheFactory,
			final Set<AccessFlags> accessFlags) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		final LoaderCache loaderCache = loaderCacheFactory.apply(attributes.getDataType());
		final T type = type(attributes.getDataType());
		return type == null
				? null
				: open(n5, dataset, blockNotFoundHandler, loaderCache, accessFlags, type);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the voxel type
	 * @param <A>
	 *            the access type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @param loaderCache
	 *            the cache
	 * @param accessFlags
	 *            the access flag set
	 * @param type
	 *            the type
	 * @return the image
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static <T extends NativeType<T>, A extends ArrayDataAccess<A>> CachedCellImg<T, A> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final LoaderCache<Long, Cell<A>> loaderCache,
			final Set<AccessFlags> accessFlags,
			final T type) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final CellGrid grid = new CellGrid(dimensions, blockSize);
		final CacheLoader<Long, Cell<A>> loader = new N5CacheLoader<>(n5, dataset, grid, type, accessFlags, blockNotFoundHandler);
		final Cache<Long, Cell<A>> cache = loaderCache.withLoader(loader);
		final CachedCellImg<T, A> img = new CachedCellImg<>(grid, type, cache, ArrayDataAccessFactory.get(type, accessFlags));
		return img;
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) {

		return open(n5, dataset, blockNotFoundHandler, AccessFlags.setOf(AccessFlags.VOLATILE));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on
	 * the number of cache entries using {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @param maxNumCacheEntries
	 *            the maximum number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries) {

		return openWithBoundedSoftRefCache(n5, dataset, blockNotFoundHandler, maxNumCacheEntries, AccessFlags.setOf(AccessFlags.VOLATILE));
	}

	/**
	 * Open an N5 mipmap (multi-scale) group as memory cached
	 * {@link LazyCellImg}s, optionally backed by {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the exception
	 * @param group
	 *            the group path
	 * @param useVolatileAccess
	 *            uses volatile access if true
	 * @param blockNotFoundHandlerSupplier
	 *            supply a consumer handling missing blocks
	 * @return the mipmap level images and their respective relative resolutions
	 */
	public static <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmapsWithHandler(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess,
			final IntFunction<Consumer<IterableInterval<T>>> blockNotFoundHandlerSupplier) {

		final int numScales = n5.list(group).length;
		@SuppressWarnings("unchecked")
		final RandomAccessibleInterval<T>[] mipmaps = new RandomAccessibleInterval[numScales];
		final double[][] scales = new double[numScales][];

		for (int s = 0; s < numScales; ++s) {
			final String datasetName = group + "/s" + s;
			final long[] dimensions = n5.getAttribute(datasetName, "dimensions", long[].class);
			final long[] downsamplingFactors = n5.getAttribute(datasetName, "downsamplingFactors", long[].class);
			final double[] scale = new double[dimensions.length];
			if (downsamplingFactors == null) {
				final int si = 1 << s;
				for (int i = 0; i < scale.length; ++i)
					scale[i] = si;
			} else {
				for (int i = 0; i < scale.length; ++i)
					scale[i] = downsamplingFactors[i];
			}

			final RandomAccessibleInterval<T> source;
			if (useVolatileAccess)
				source = N5Utils.openVolatile(n5, datasetName, blockNotFoundHandlerSupplier.apply(s));
			else
				source = N5Utils.open(n5, datasetName, blockNotFoundHandlerSupplier.apply(s));

			mipmaps[s] = source;
			scales[s] = scale;
		}

		return new ValuePair<>(mipmaps, scales);
	}

	/**
	 * Open an N5 mipmap (multi-scale) group as memory cached
	 * {@link LazyCellImg}s, optionally backed by {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param group
	 *            the group path
	 * @param useVolatileAccess
	 *            uses volatile access if true
	 * @param defaultValueSupplier
	 *            supplies a default value
	 * @return the mipmap level images and their respective relative resolutions
	 */
	public static <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmaps(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess,
			final IntFunction<T> defaultValueSupplier) {

		return openMipmapsWithHandler(
				n5,
				group,
				useVolatileAccess,
				s -> {
					return N5CacheLoader.setToDefaultValue(defaultValueSupplier.apply(s));
				});
	}

	/**
	 * Open an N5 mipmap (multi-scale) group as memory cached
	 * {@link LazyCellImg}s, optionally backed by {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param group
	 *            the group path
	 * @param useVolatileAccess
	 *            uses volatile access if true
	 * @return the mipmap level images and their respective relative resolutions
	 */
	public static <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmaps(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess) {

		return openMipmapsWithHandler(
				n5,
				group,
				useVolatileAccess,
				s -> t -> {});
	}

	/**
	 * Open an N5 dataset as a disk-cached {@link LazyCellImg}. Note that this
	 * requires that all parts of the N5 dataset that will be accessed fit
	 * into /tmp.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @return the image
	 */
	public static <T extends NativeType<T>, A extends ArrayDataAccess<A>> CachedCellImg<T, ?> openWithDiskCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final CellGrid grid = new CellGrid(dimensions, blockSize);
		final T type = type(attributes.getDataType());
		final Set<AccessFlags> accessFlags = AccessFlags.setOf(AccessFlags.VOLATILE, AccessFlags.DIRTY);
		final CacheLoader<Long, Cell<A>> loader = new N5CacheLoader<>(n5, dataset, grid, type, accessFlags, blockNotFoundHandler);

		final DiskCachedCellImgOptions options = DiskCachedCellImgOptions
				.options()
				.cellDimensions(blockSize)
				.dirtyAccesses(true)
				.maxCacheSize(100);

		final DiskCachedCellImgFactory<T> factory = new DiskCachedCellImgFactory<T>(
				type, options);

		return factory.createWithCacheLoader(dimensions, loader);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 * @param gridOffset
	 *            the offset of the source in the larger dataset
	 */
	public static <T extends NativeType<T>> void saveBlock(
			RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset) {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset)) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultisetBlock(labelMultisetSource, n5, dataset, attributes, gridOffset);
			return;
		}

		final RandomAccessibleInterval<Interval> gridBlocks = new CellGrid(source.dimensionsAsLongArray(), attributes.getBlockSize())
				.cellIntervals()
				.view().translate(gridOffset);
		final BlockWriter writer = BlockWriter.create(source.view().zeroMin(), n5, dataset, attributes);
		Streams.localizing(gridBlocks)
				.map(writer::writeTask)
				.forEach(Runnable::run);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 */
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes) {

		final int[] blockSize = attributes.getBlockSize();
		final long[] gridOffset = new long[blockSize.length];
		Arrays.setAll(gridOffset, d -> source.min(d) / blockSize[d]);
		saveBlock(source, n5, dataset, attributes, gridOffset);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 */
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveBlock(source, n5, dataset, attributes);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the position in the block grid
	 */
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveBlock(source, n5, dataset, attributes, gridOffset);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset, multi-threaded. The offset is given in {@link DataBlock} grid
	 * coordinates and the source is assumed to align with the {@link DataBlock}
	 * grid of the dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 * @param gridOffset
	 *            the position in the block grid
	 * @param exec
	 *            the executor service
	 * @throws InterruptedException
	 *             the interrupted exception
	 * @throws ExecutionException
	 *             the execution exception
	 */
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset)) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultisetBlock(labelMultisetSource, n5, dataset, gridOffset, exec);
			return;
		}

		final RandomAccessibleInterval<Interval> gridBlocks = new CellGrid(source.dimensionsAsLongArray(), attributes.getBlockSize())
				.cellIntervals()
				.view().translate(gridOffset);
		final BlockWriter writer = BlockWriter.create(source.view().zeroMin(), n5, dataset, attributes).threadSafe();
		final List<Future<?>> futures = Streams.localizing(gridBlocks)
				.map(writer::writeTask)
				.map(exec::submit)
				.collect(Collectors.toList());
		for (final Future<?> f : futures)
			f.get();
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset, multi-threaded. The offset is given in {@link DataBlock} grid
	 * coordinates and the source is assumed to align with the {@link DataBlock}
	 * grid of the dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the position in the block grid
	 * @param exec
	 *            the executor service
	 * @throws InterruptedException
	 *             the interrupted exception
	 * @throws ExecutionException
	 *             the execution exception
	 */
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveBlock(source, n5, dataset, attributes, gridOffset, exec);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset. Only {@link DataBlock DataBlocks} that contain values other than
	 * a given default value are stored.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 * @param gridOffset
	 *            the position in the block grid
	 * @param defaultValue
	 *            the default value
	 */
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset,
			final T defaultValue) {

		final RandomAccessibleInterval<Interval> gridBlocks = new CellGrid(source.dimensionsAsLongArray(), attributes.getBlockSize())
				.cellIntervals()
				.view().translate(gridOffset);
		final BlockWriter writer = BlockWriter.createNonEmpty(source.view().zeroMin(), n5, dataset, attributes, defaultValue);
		Streams.localizing(gridBlocks)
				.map(writer::writeTask)
				.forEach(Runnable::run);

		N5BlockValidateAndRetry.validateAndRetry(source, n5, dataset, gridOffset, defaultValue);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset. Only
	 * {@link DataBlock DataBlocks} that contain values other than a given
	 * default value are stored.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 * @param defaultValue
	 *            the default value
	 */
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final T defaultValue) {

		final int[] blockSize = attributes.getBlockSize();
		final long[] gridOffset = new long[blockSize.length];
		Arrays.setAll(gridOffset, d -> source.min(d) / blockSize[d]);
		saveNonEmptyBlock(source, n5, dataset, attributes, gridOffset, defaultValue);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset. Only
	 * {@link DataBlock DataBlocks} that contain values other than a given
	 * default value are stored.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 */
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final T defaultValue) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveNonEmptyBlock(source, n5, dataset, attributes, defaultValue);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset. Only {@link DataBlock DataBlocks} that contain values other than
	 * a given default value are stored.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the position in the block grid
	 * @param defaultValue
	 *            the default value
	 */
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final T defaultValue) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveNonEmptyBlock(source, n5, dataset, attributes, gridOffset, defaultValue);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset.
	 *
	 * @param <T>
	 *            the type parameter
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
	 */
	public static <T extends NativeType<T>> void save(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression) {

		if (source.getType() instanceof LabelMultisetType) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultiset(labelMultisetSource, n5, dataset, blockSize, compression);
			return;
		}

		final DatasetAttributes attributes = new DatasetAttributes(
				source.dimensionsAsLongArray(),
				blockSize,
				dataType(source.getType()),
				compression);
		n5.createDataset(dataset, attributes);
		saveBlock(source, n5, dataset, attributes);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset, multi-threaded.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param blockSize
	 *            the block size
	 * @param compression
	 *            the compression type
	 * @param exec
	 *            executor for parallel writing
	 * @throws InterruptedException
	 *             the interrupted exception
	 * @throws ExecutionException
	 *             the execution exception
	 */
	public static <T extends NativeType<T>> void save(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		if (source.getType() instanceof LabelMultisetType) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultiset(labelMultisetSource, n5, dataset, blockSize, compression, exec);
			return;
		}

		final DatasetAttributes attributes = new DatasetAttributes(
				source.dimensionsAsLongArray(),
				blockSize,
				dataType(source.getType()),
				compression);
		n5.createDataset(dataset, attributes);
		final long[] gridOffset = new long[source.numDimensions()];
		saveBlock(source, n5, dataset, attributes, gridOffset, exec);
	}

	/**
	 * Write an image into an existing n5 dataset, padding the dataset if
	 * necessary. The min and max values of the input source interval define the
	 * subset of the dataset to be written.
	 *
	 * Warning! Avoid calling this method in parallel for multiple sources that
	 * have blocks in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset
	 * @throws ExecutionException
	 *             the execution exception
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	public static <T extends NativeType<T>> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset) throws InterruptedException, ExecutionException {

		saveRegion(source, n5, dataset, n5.getDatasetAttributes(dataset));
	}

	/**
	 * Write an image into an existing n5 dataset, padding the dataset if
	 * necessary. The min and max values of the input source interval define the
	 * subset of the dataset to be written. Blocks of the output at written in
	 * parallel using the given {@link ExecutorService}.
	 *
	 * Warning! Avoid calling this method in parallel for multiple sources that
	 * have blocks in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset
	 * @param exec
	 *            executor service
	 * @throws ExecutionException
	 *             the execution exception
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	public static <T extends NativeType<T>> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		saveRegion(source, n5, dataset, n5.getDatasetAttributes(dataset), exec);
	}

	/**
	 * Write an image into an existing n5 dataset, padding the dataset if
	 * necessary. The min and max values of the input source interval define the
	 * subset of the dataset to be written.
	 *
	 * Warning! Avoid calling this method in parallel for multiple sources that
	 * have blocks in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset
	 * @param attributes
	 *            dataset attributes
	 * @throws ExecutionException
	 *             the execution exception
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	public static <T extends NativeType<T>, P> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes) throws InterruptedException, ExecutionException {

		final Optional<long[]> newDimensionsOpt = saveRegionPreprocessing(source, attributes);

		final long[] dimensions;
		if (newDimensionsOpt.isPresent()) {
			n5.setAttribute(dataset, "dimensions", newDimensionsOpt.get());
			dimensions = newDimensionsOpt.get();
		} else {
			dimensions = attributes.getDimensions();
		}

		// find the grid positions bounding the source image to save
		final RandomAccessibleInterval<Interval> gridBlocks = findBoundingGridBlocks(
				source, dimensions, attributes.getBlockSize());

		// iterate over those blocks
		final RegionBlockWriter writer = RegionBlockWriter.create(source, n5, dataset, attributes);
		Streams.localizing(gridBlocks)
				.map(writer::writeTask)
				.forEach(Runnable::run);
	}

	/**
	 * Write an image into an existing n5 dataset, padding the dataset if
	 * necessary. The min and max values of the input source interval define the
	 * subset of the dataset to be written. Blocks of the output at written in
	 * parallel using the given {@link ExecutorService}.
	 *
	 * Warning! Avoid calling this method in parallel for multiple sources that
	 * have blocks in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset
	 * @param attributes
	 *            dataset attributes
	 * @param exec
	 *            the executor
	 * @throws ExecutionException
	 *             the execution exception
	 * @throws InterruptedException
	 *             the interrupted exception
	 *
	 */
	public static <T extends NativeType<T>, P> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		final Optional<long[]> newDimensionsOpt = saveRegionPreprocessing(source, attributes);

		final long[] dimensions;
		if (newDimensionsOpt.isPresent()) {
			n5.setAttribute(dataset, "dimensions", newDimensionsOpt.get());
			dimensions = newDimensionsOpt.get();
		} else {
			dimensions = attributes.getDimensions();
		}

		// find the grid positions bounding the source image to save
		final RandomAccessibleInterval<Interval> gridBlocks = findBoundingGridBlocks(
				source, dimensions, attributes.getBlockSize());

		// iterate over those blocks
		final RegionBlockWriter writer = RegionBlockWriter.create(source, n5, dataset, attributes).threadSafe();
		final List<Future<?>> futures = Streams.localizing(gridBlocks)
				.map(writer::writeTask)
				.map(exec::submit)
				.collect(Collectors.toList());
		for (final Future<?> f : futures)
			f.get();
	}

	/**
	 * Performs checks, and determine if padding is necessary.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image to write
	 * @param attributes
	 *            n5 dataset attributes
	 * @return new dataset dimensions if padding necessary, empty optional
	 *         otherwise
	 */
	private static <T extends NativeType<T>> Optional<long[]> saveRegionPreprocessing(
			final RandomAccessibleInterval<T> source,
			final DatasetAttributes attributes) {

		final DataType dtype = attributes.getDataType();
		final long[] currentDimensions = attributes.getDimensions();
		final int n = currentDimensions.length;

		// ensure source has the correct dimensionality
		if (source.numDimensions() != n) {
			throw new ImgLibException(
					String.format("Image dimensions (%d) does not match n5 dataset dimensionalidy (%d)",
							source.numDimensions(), n));
		}

		// ensure type of passed image matches the existing dataset
		final DataType srcType = N5Utils.dataType(source.getType());
		if (srcType != dtype) {
			throw new ImgLibException(
					String.format("Image type (%s) does not match n5 dataset type (%s)",
							srcType, dtype));
		}

		// check if the volume needs padding
		// and that the source min is >= 0
		boolean needsPadding = false;
		final long[] newDimensions = new long[n];

		// set newDimensions to current dimensions
		for (int d = 0; d < n; d++) {
			if (source.min(d) < 0) {
				throw new ImgLibException(
						String.format("Source interval min (%d) in dimension %d must be >= 0",
								source.min(d), d));
			}

			if (source.max(d) + 1 > currentDimensions[d]) {
				newDimensions[d] = source.max(d) + 1;
				needsPadding = true;
			} else {
				newDimensions[d] = currentDimensions[d];
			}
		}

		if (needsPadding)
			return Optional.of(newDimensions);
		else
			return Optional.empty();
	}

	/**
	 * Find the grid positions of DataBlocks overlapping the {@code sourceInterval}.
	 * The position of a {@code RandomAccess} is the gridPosition of a block.
	 * {@code RandomAccess.get()} gives the interval covered by the block.
	 *
	 * @param sourceInterval
	 * 		source interval to cover
	 * @param datasetDimensions
	 * 		dimensions of the dataset (must fully contain source)
	 * @param blockSize
	 * 		blocksize of the dataset
	 *
	 * @return a {@code RandomAccessibleInterval} of the grid blocks (intervals) overlapping the {@code sourceInterval}.
	 */
	private static RandomAccessibleInterval<Interval> findBoundingGridBlocks(
			final Interval sourceInterval,
			final long[] datasetDimensions,
			final int[] blockSize
	) {
		// find the grid positions bounding the source image to save
		final int n = sourceInterval.numDimensions();
		final long[] gridMin = new long[n];
		final long[] gridMax = new long[n];
		for (int d = 0; d < n; d++) {
			gridMin[d] = Math.floorDiv(sourceInterval.min(d), blockSize[d]);
			gridMax[d] = Math.floorDiv(sourceInterval.max(d), blockSize[d]);
		}
		return new CellGrid(datasetDimensions, blockSize)
				.cellIntervals()
				.view().interval(FinalInterval.wrap(gridMin, gridMax));
	}

	/**
	 * Delete an {@link Interval} in an N5 dataset at a given offset. The offset
	 * is given in {@link DataBlock} grid coordinates and the interval is
	 * assumed to align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param interval
	 *            the interval
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            dataset attributes
	 * @param gridOffset
	 *            the position in the block grid
	 */
	// TODO: the interval is assumed to be zero-min in this method.
	//       Should we change the argument type to Dimensions to make that more obvious?>
	public static void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset) {

		final RandomAccessibleInterval<Interval> gridBlocks = new CellGrid(interval.dimensionsAsLongArray(), attributes.getBlockSize())
				.cellIntervals()
				.view().translate(gridOffset);
		Streams.localizing(gridBlocks)
				.forEach(b -> n5.deleteBlock(dataset, b.positionAsLongArray()));

	}

	/**
	 * Delete an {@link Interval} in an N5 dataset. The block offset is
	 * determined by the interval position, and the interval is assumed to align
	 * with the {@link DataBlock} grid of the dataset.
	 *
	 * @param interval
	 *            the interval
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            dataset attributes
	 */
	public static void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes) {

		final int[] blockSize = attributes.getBlockSize();
		final long[] gridOffset = new long[blockSize.length];
		Arrays.setAll(gridOffset, d -> interval.min(d) / blockSize[d]);
		deleteBlock(interval, n5, dataset, attributes, gridOffset);
	}

	/**
	 * Delete an {@link Interval} in an N5 dataset. The block offset is
	 * determined by the interval position, and the interval is assumed to align
	 * with the {@link DataBlock} grid of the dataset.
	 *
	 * @param interval
	 *            the interval
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 */
	public static void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			deleteBlock(interval, n5, dataset, attributes);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Delete an {@link Interval} in an N5 dataset at a given offset. The offset
	 * is given in {@link DataBlock} grid coordinates and the interval is
	 * assumed to align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param interval
	 *            the interval
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the position in the block grid
	 */
	public static void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			deleteBlock(interval, n5, dataset, attributes, gridOffset);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}


	/**
	 * Write DataBlocks from a source image that aligns with the {@link
	 * DataBlock} grid of the dataset.
	 */
	private interface BlockWriter {

		static <T extends NativeType<T>> BlockWriter create(
				final RandomAccessibleInterval<T> source,
				final N5Writer n5,
				final String dataset,
				final DatasetAttributes attributes) {

			return new Imp<>(source, attributes.getDataType(), dataBlock ->
					n5.writeBlock(dataset, attributes, dataBlock));
		}

		static <T extends NativeType<T>> BlockWriter createNonEmpty(
				final RandomAccessibleInterval<T> source,
				final N5Writer n5,
				final String dataset,
				final DatasetAttributes attributes,
				final T defaultValue) {

			return new Imp<>(source, attributes.getDataType(), dataBlock -> {
				if (!allEqual(defaultValue, dataBlock.getData()))
					n5.writeBlock(dataset, attributes, dataBlock);
			});
		}

		/**
		 * Write a DataBlock at {@code gridPos}.
		 * <p>
		 * The interval covered by the block in the source image is given by
		 * {@code blockMin} and {@code blockSize}. It must be fully inside the
		 * source image.
		 *
		 * @param gridPos
		 * 		the grid coordinates of the block
		 * @param blockMin
		 * 		minimum of the interval covered by the block in the source image
		 * @param blockSize
		 * 		dimensions of the interval covered by the block in the source image
		 */
		void write(long[] gridPos, long[] blockMin, int[] blockSize);

		default Runnable writeTask(LocalizableSampler<Interval> gridBlock) {
			final long[] gridPos = gridBlock.positionAsLongArray();
			final Interval blockInterval = gridBlock.get();
			final long[] blockMin = blockInterval.minAsLongArray();
			final int[] blockSize = new int[ blockInterval.numDimensions() ];
			Arrays.setAll(blockSize, d -> (int) blockInterval.dimension(d));
			return () -> write(gridPos, blockMin, blockSize);
		}

		/**
		 * Get a thread-safe version of this {@code RegionBlockWriter}.
		 * (Implemented as a wrapper that makes {@link ThreadLocal} copies).
		 */
		default BlockWriter threadSafe() {return this;}

		class Imp<T extends NativeType<T>, P> implements BlockWriter {

			final DataType dataType;
			final Consumer<DataBlock<P>> writeBlock;
			final PrimitiveBlocks<T> sourceBlocks;
			final int[] zeroPos;

			Imp(
					final RandomAccessibleInterval<T> source,
					final DataType dataType,
					final Consumer<DataBlock<P>> writeBlock) {
				this.dataType = dataType;
				this.writeBlock = writeBlock;
				sourceBlocks = PrimitiveBlocks.of(source, OnFallback.ACCEPT);
				final int n = source.numDimensions();
				zeroPos = new int[n];
			}

			Imp(final Imp<T, P> writer) {
				this.dataType = writer.dataType;
				this.writeBlock = writer.writeBlock;
				this.sourceBlocks = writer.sourceBlocks.independentCopy();
				this.zeroPos = writer.zeroPos;
			}

			@Override
			public void write(final long[] gridPos, final long[] blockMin, final int[] blockSize) {
				final DataBlock<P> dataBlock = Cast.unchecked(dataType.createDataBlock(blockSize, gridPos));
				sourceBlocks.copy(blockMin, dataBlock.getData(), blockSize);
				writeBlock.accept(dataBlock);
			}

			private Supplier<Imp<T, P>> threadSafeSupplier;

			@Override
			public BlockWriter threadSafe() {
				if (threadSafeSupplier == null)
					threadSafeSupplier = CloseableThreadLocal.withInitial(() -> new Imp<>(this))::get;
				return (gridPos, blockMin, blockSize) -> threadSafeSupplier.get().write(gridPos, blockMin, blockSize);
			}
		}
	}


	/**
	 * Write (or override) a DataBlocks which may fully or partially overlap the
	 * source image. In the latter case only a part of the block is filled (or
	 * overridden) with data.
	 */
	private interface RegionBlockWriter {

		static <T extends NativeType<T>> RegionBlockWriter create(
				final RandomAccessibleInterval<T> source,
				final N5Writer n5,
				final String dataset,
				final DatasetAttributes attributes) {

			return new Imp<>(source, attributes.getDataType(),
					gridPosition -> Cast.unchecked(n5.readBlock(dataset, attributes, gridPosition)),
					dataBlock -> n5.writeBlock(dataset, attributes, dataBlock));
		}

		/**
		 * Write (or override) a DataBlock at {@code gridPos}.
		 * <p>
		 * The interval covered by the block in the source image is given by
		 * {@code blockInterval}. {@code blockInterval} might only partially
		 * overlap the source image. In that cas only a part of the block is
		 * filled (or overridden) with data.
		 *
		 * @param gridPos
		 * 		the grid coordinates of the block
		 * @param blockMin
		 * 		minimum of the interval covered by the block in the source image
		 * @param blockSize
		 * 		dimensions of the interval covered by the block in the source image
		 */
		void write(long[] gridPos, long[] blockMin, int[] blockSize);

		default Runnable writeTask(LocalizableSampler<Interval> gridBlock) {
			final long[] gridPos = gridBlock.positionAsLongArray();
			final Interval blockInterval = gridBlock.get();
			final long[] blockMin = blockInterval.minAsLongArray();
			final int[] blockSize = new int[blockInterval.numDimensions()];
			Arrays.setAll(blockSize, d -> (int) blockInterval.dimension(d));
			return () -> write(gridPos, blockMin, blockSize);
		}

		/**
		 * Get a thread-safe version of this {@code RegionBlockWriter}.
		 * (Implemented as a wrapper that makes {@link ThreadLocal} copies).
		 */
		default RegionBlockWriter threadSafe() {return this;}

		class Imp<T extends NativeType<T>, P> implements RegionBlockWriter {

			private final DataType dataType;
			private final Function<long[], DataBlock<P>> readBlock;
			private final Consumer<DataBlock<P>> writeBlock;

			private final Interval sourceInterval;
			private final PrimitiveBlocks<T> sourceBlocks;
			private final SubArrayCopy.Typed<P, P> subArrayCopy;
			private final TempArray<P> tempArray;

			private final int[] zeroPos;
			private final long[] intersectionMin;
			private final int[] intersectionSize;
			private final int[] intersectionOffset;

			Imp(final RandomAccessibleInterval<T> source,
					final DataType dataType,
					final Function<long[], DataBlock<P>> readBlock,
					final Consumer<DataBlock<P>> writeBlock) {
				this.dataType = dataType;
				this.readBlock = readBlock;
				this.writeBlock = writeBlock;

				this.sourceInterval = source;
				sourceBlocks = PrimitiveBlocks.of(source, OnFallback.ACCEPT);
				final PrimitiveType p = source.getType().getNativeTypeFactory().getPrimitiveType();
				subArrayCopy = SubArrayCopy.forPrimitiveType(p);
				tempArray = TempArray.forPrimitiveType(p);

				final int n = source.numDimensions();
				zeroPos = new int[n];
				intersectionMin = new long[n];
				intersectionSize = new int[n];
				intersectionOffset = new int[n];
			}

			private Imp(final Imp<T, P> writer) {
				this.dataType = writer.dataType;
				this.readBlock = writer.readBlock;
				this.writeBlock = writer.writeBlock;

				this.sourceInterval = writer.sourceInterval;
				this.sourceBlocks = writer.sourceBlocks.independentCopy();
				this.subArrayCopy = writer.subArrayCopy;
				this.tempArray = writer.tempArray.newInstance();

				this.zeroPos = writer.zeroPos;
				final int n = writer.zeroPos.length;
				this.intersectionMin = new long[n];
				this.intersectionSize = new int[n];
				this.intersectionOffset = new int[n];
			}

			@Override
			public void write(final long[] gridPos, final long[] blockMin, final int[] blockSize) {

				final int n = gridPos.length;
				for (int d = 0; d < n; d++) {
					intersectionMin[d] = Math.max(sourceInterval.min(d), blockMin[d]);
					intersectionSize[d] = (int) (Math.min(sourceInterval.max(d) + 1, blockMin[d] + blockSize[d]) - intersectionMin[d]);
				}

				if (Arrays.equals(intersectionSize, blockSize)) {
					// Full overlap: Fill a new DataBlock with source data.
					// (It doesn't matter, whether a block already exists at gridPos, we would override everything anyway.)
					final DataBlock<P> dataBlock = Cast.unchecked(dataType.createDataBlock(blockSize, gridPos));
					sourceBlocks.copy(blockMin, dataBlock.getData(), blockSize);
					writeBlock.accept(dataBlock);
				} else {
					final DataBlock<P> dataBlock;
					// Partial overlap: Try to read the DataBlock at gridPos.
					final DataBlock<P> existingBlock = readBlock.apply(gridPos);
					if (existingBlock == null) {
						// There is no existing DataBlock. Create a new one.
						dataBlock = Cast.unchecked(dataType.createDataBlock(blockSize, gridPos));
					} else {
						// There is an existing DataBlock. Is it large enough?
						// Perhaps it was a truncated border block, and now we
						// expanded the dataset.
						if (Arrays.equals(existingBlock.getSize(), blockSize)) {
							dataBlock = existingBlock;
						} else {
							// Create a new DataBlock and copy existing data over.
							dataBlock = Cast.unchecked(dataType.createDataBlock(blockSize, gridPos));
							subArrayCopy.copy(
									existingBlock.getData(), existingBlock.getSize(), zeroPos,
									dataBlock.getData(), dataBlock.getSize(), zeroPos, existingBlock.getSize());
						}
					}
					// Copy intersecting portion of source data into the DataBlock
					final P sourceData = tempArray.get((int) Intervals.numElements(intersectionSize));
					sourceBlocks.copy(intersectionMin, sourceData, intersectionSize);
					Arrays.setAll(intersectionOffset, d -> (int) (intersectionMin[d] - blockMin[d]));
					subArrayCopy.copy(
							sourceData, intersectionSize, zeroPos,
							dataBlock.getData(), dataBlock.getSize(), intersectionOffset, intersectionSize);
					writeBlock.accept(dataBlock);
				}
			}

			private Supplier<Imp<T, P>> threadSafeSupplier;

			@Override
			public RegionBlockWriter threadSafe() {
				if (threadSafeSupplier == null)
					threadSafeSupplier = CloseableThreadLocal.withInitial(() -> new Imp<>(this))::get;
				return (gridPos, blockMin, blockSize) -> threadSafeSupplier.get().write(gridPos, blockMin, blockSize);
			}
		}
	}


	/**
	 * @return primitve array with one element corresponding to the given value
	 */
	private static < T extends NativeType< T > > Object extractValue( final T value )
	{
		final ArrayImg< T, ? > img = new ArrayImgFactory<>( value ).create( 1 );
		img.firstElement().set( value );
		return ( ( ArrayDataAccess< ? > ) ( img.update( null ) ) ).getCurrentStorageArray();
	}

	/**
	 * @return {@code true} if all elements of {@code data} are equal to {@code value}
	 */
	private static <T extends NativeType<T>> boolean allEqual(T value, Object data) {
		final PrimitiveType primitiveType = value.getNativeTypeFactory().getPrimitiveType();
		final Object valueArray = extractValue(value);
		switch (primitiveType) {
		case BOOLEAN: {
			final boolean v = ((boolean[]) valueArray)[0];
			final boolean[] booleans = (boolean[]) data;
			for (int i = 0; i < booleans.length; ++i) {
				if (booleans[i] != v) {
					return false;
				}
			}
			return true;
		}
		case BYTE: {
			final byte v = ((byte[]) valueArray)[0];
			final byte[] bytes = (byte[]) data;
			for (int i = 0; i < bytes.length; ++i) {
				if (bytes[i] != v) {
					return false;
				}
			}
			return true;
		}
		case CHAR: {
			final char v = ((char[]) valueArray)[0];
			final char[] chars = (char[]) data;
			for (int i = 0; i < chars.length; ++i) {
				if (chars[i] != v) {
					return false;
				}
			}
			return true;
		}
		case SHORT: {
			final short v = ((short[]) valueArray)[0];
			final short[] shorts = (short[]) data;
			for (int i = 0; i < shorts.length; ++i) {
				if (shorts[i] != v) {
					return false;
				}
			}
			return true;

		}
		case INT: {
			final int v = ((int[]) valueArray)[0];
			final int[] ints = (int[]) data;
			for (int i = 0; i < ints.length; ++i) {
				if (ints[i] != v) {
					return false;
				}
			}
			return true;

		}
		case LONG: {
			final long v = ((long[]) valueArray)[0];
			final long[] longs = (long[]) data;
			for (int i = 0; i < longs.length; ++i) {
				if (longs[i] != v) {
					return false;
				}
			}
			return true;

		}
		case FLOAT: {
			final float v = ((float[]) valueArray)[0];
			final float[] floats = (float[]) data;
			for (int i = 0; i < floats.length; ++i) {
				if (floats[i] != v) {
					return false;
				}
			}
			return true;

		}
		case DOUBLE: {
			final double v = ((double[]) valueArray)[0];
			final double[] doubles = (double[]) data;
			for (int i = 0; i < doubles.length; ++i) {
				if (doubles[i] != v) {
					return false;
				}
			}
			return true;
		}
		default:
			throw new UnsupportedOperationException();
		}
	}
}
