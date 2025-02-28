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

import static net.imglib2.img.basictypeaccess.AccessFlags.DIRTY;
import static net.imglib2.img.basictypeaccess.AccessFlags.VOLATILE;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

import net.imglib2.blocks.SubArrayCopy;
import net.imglib2.type.Type;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;

import net.imglib2.IterableInterval;
import net.imglib2.cache.CacheLoader;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.img.basictypeaccess.array.DirtyByteArray;
import net.imglib2.img.basictypeaccess.array.DirtyDoubleArray;
import net.imglib2.img.basictypeaccess.array.DirtyFloatArray;
import net.imglib2.img.basictypeaccess.array.DirtyIntArray;
import net.imglib2.img.basictypeaccess.array.DirtyLongArray;
import net.imglib2.img.basictypeaccess.array.DirtyShortArray;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.IntArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.basictypeaccess.array.ShortArray;
import net.imglib2.img.basictypeaccess.volatiles.array.DirtyVolatileByteArray;
import net.imglib2.img.basictypeaccess.volatiles.array.DirtyVolatileDoubleArray;
import net.imglib2.img.basictypeaccess.volatiles.array.DirtyVolatileFloatArray;
import net.imglib2.img.basictypeaccess.volatiles.array.DirtyVolatileIntArray;
import net.imglib2.img.basictypeaccess.volatiles.array.DirtyVolatileLongArray;
import net.imglib2.img.basictypeaccess.volatiles.array.DirtyVolatileShortArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileByteArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileDoubleArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileFloatArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileIntArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileLongArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileShortArray;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.PrimitiveType;
import net.imglib2.util.Cast;
import net.imglib2.util.Intervals;

/**
 * A {@link CacheLoader} for N5 dataset blocks. Supports all primitive types.
 *
 * @param <T>
 *            pixel type
 * @param <A>
 *            access type
 *
 * @author Tobias Pietzsch
 */
public class N5CacheLoader<T extends NativeType<T>, A extends ArrayDataAccess<A>> implements CacheLoader<Long, Cell<A>> {

	private final N5Reader n5;
	private final String dataset;
	private final DatasetAttributes attributes;
	private final CellGrid grid;
	private final ArrayDataAccessLoader<?, A> cacheArrayLoader;
	private final CacheLoader<Long, Cell<A>> missingLoader;

	public N5CacheLoader(
			final N5Reader n5,
			final String dataset,
			final CellGrid grid,
			final T type,
			final Set<AccessFlags> accessFlags,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) throws N5Exception {

		this.n5 = n5;
		this.dataset = dataset;
		this.grid = grid;
		attributes = n5.getDatasetAttributes(dataset);
		cacheArrayLoader = createN5CacheArrayLoader(type, accessFlags);
		missingLoader = LoadedCellCacheLoader.get(grid, blockNotFoundHandler::accept, type, accessFlags);
	}

	@Override
	public Cell<A> get(final Long key) throws Exception {

		final int n = grid.numDimensions();
		final long[] cellGridPosition = new long[n];
		grid.getCellGridPositionFlat(key, cellGridPosition);
		final DataBlock<?> dataBlock = n5.readBlock(dataset, attributes, cellGridPosition);
		if (dataBlock != null) {
			final long[] cellMin = new long[n];
			final int[] cellDims = new int[n];
			grid.getCellDimensions(key, cellMin, cellDims);
			final A data = cacheArrayLoader.loadArray(Cast.unchecked(dataBlock), cellDims);
			return new Cell<>(cellDims, cellMin, data);
		} else
			return missingLoader.get(key);
	}

	private static class ArrayDataAccessLoader<P, A> {

		private final IntFunction<P> createPrimitiveArray;
		private final Function<P, A> createArrayAccess;
		private final SubArrayCopy.Typed<P,P> subArrayCopy;

		ArrayDataAccessLoader(
				final IntFunction<P> createPrimitiveArray,
				final SubArrayCopy.Typed<P,P> subArrayCopy,
				final Function<P, A> createArrayAccess) {

			this.createPrimitiveArray = createPrimitiveArray;
			this.createArrayAccess = createArrayAccess;
			this.subArrayCopy = subArrayCopy;
		}

		public A loadArray(final DataBlock<P> dataBlock, final int[] cellDimensions) {

			final int[] dataBlockSize = dataBlock.getSize();
			if (Arrays.equals(dataBlockSize, cellDimensions)) {
				return createArrayAccess.apply(dataBlock.getData());
			} else {
				final P data = createPrimitiveArray.apply((int)Intervals.numElements(cellDimensions));
				final P src = dataBlock.getData();
				final int[] pos = new int[dataBlockSize.length];
				final int[] size = new int[dataBlockSize.length];
				Arrays.setAll(size, d -> Math.min(dataBlockSize[d], cellDimensions[d]));
				subArrayCopy.copy(src, dataBlockSize, pos,data, cellDimensions, pos, size);
				return createArrayAccess.apply(data);
			}
		}
	}

	private static <T extends NativeType<T>> ArrayDataAccessLoader createN5CacheArrayLoader(
			final T type,
			final Set<AccessFlags> accessFlags) {

		final boolean dirty = accessFlags.contains(DIRTY);
		final boolean volatil = accessFlags.contains(VOLATILE);
		final PrimitiveType primitiveType = type.getNativeTypeFactory().getPrimitiveType();
		switch (primitiveType) {
		case BYTE:
			return new ArrayDataAccessLoader<>(byte[]::new,
					SubArrayCopy.forPrimitiveType(primitiveType),
					dirty
							? volatil
									? data -> new DirtyVolatileByteArray(data, true)
									: data -> new DirtyByteArray(data)
							: volatil
									? data -> new VolatileByteArray(data, true)
									: data -> new ByteArray(data));
		case SHORT:
			return new ArrayDataAccessLoader<>(short[]::new,
					SubArrayCopy.forPrimitiveType(primitiveType),
					dirty
							? volatil
									? data -> new DirtyVolatileShortArray(data, true)
									: data -> new DirtyShortArray(data)
							: volatil
									? data -> new VolatileShortArray(data, true)
									: data -> new ShortArray(data));
		case INT:
			return new ArrayDataAccessLoader<>(int[]::new,
					SubArrayCopy.forPrimitiveType(primitiveType),
					dirty
							? volatil
									? data -> new DirtyVolatileIntArray(data, true)
									: data -> new DirtyIntArray(data)
							: volatil
									? data -> new VolatileIntArray(data, true)
									: data -> new IntArray(data));
		case LONG:
			return new ArrayDataAccessLoader<>(long[]::new,
					SubArrayCopy.forPrimitiveType(primitiveType),
					dirty
							? volatil
									? data -> new DirtyVolatileLongArray(data, true)
									: data -> new DirtyLongArray(data)
							: volatil
									? data -> new VolatileLongArray(data, true)
									: data -> new LongArray(data));
		case FLOAT:
			return new ArrayDataAccessLoader<>(float[]::new,
					SubArrayCopy.forPrimitiveType(primitiveType),
					dirty
							? volatil
									? data -> new DirtyVolatileFloatArray(data, true)
									: data -> new DirtyFloatArray(data)
							: volatil
									? data -> new VolatileFloatArray(data, true)
									: data -> new FloatArray(data));
		case DOUBLE:
			return new ArrayDataAccessLoader<>(double[]::new,
					SubArrayCopy.forPrimitiveType(primitiveType),
					dirty
							? volatil
									? data -> new DirtyVolatileDoubleArray(data, true)
									: data -> new DirtyDoubleArray(data)
							: volatil
									? data -> new VolatileDoubleArray(data, true)
									: data -> new DoubleArray(data));
		default:
			throw new IllegalArgumentException();
		}
	}

	/**
	 *
	 * @param <T>
	 *            type parameter
	 * @param <I>
	 *            interval type
	 * @param defaultValue
	 *            the default value
	 * @return {@link Consumer} that sets all values of its argument to
	 *         {@code defaultValue}.
	 */
	public static <T extends Type<T>, I extends IterableInterval<T>> Consumer<I> setToDefaultValue(
			final T defaultValue) {

		return rai -> rai.forEach(pixel -> pixel.set(defaultValue));
	}
}
