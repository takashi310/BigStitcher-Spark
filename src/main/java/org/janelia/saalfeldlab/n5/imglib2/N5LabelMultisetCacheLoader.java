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

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.BiFunction;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.label.AbstractLabelMultisetLoader;
import net.imglib2.type.label.ByteUtils;
import net.imglib2.type.label.LabelMultisetEntry;
import net.imglib2.type.label.LabelMultisetEntryList;
import net.imglib2.type.label.LongMappedAccessData;

public class N5LabelMultisetCacheLoader extends AbstractLabelMultisetLoader {

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final N5Reader n5;

	private final String dataset;

	private final BiFunction<CellGrid, long[], byte[]> nullReplacement;

	public N5LabelMultisetCacheLoader(
			final N5Reader n5,
			final String dataset) {

		this(n5, dataset, (g, p) -> null);
	}

	public N5LabelMultisetCacheLoader(
			final N5Reader n5,
			final String dataset,
			final BiFunction<CellGrid, long[], byte[]> nullReplacement) {

		super(generateCellGrid(n5, dataset));
		this.n5 = n5;
		this.dataset = dataset;
		this.nullReplacement = nullReplacement;
	}

	public static BiFunction<CellGrid, long[], byte[]> constantNullReplacement(final long id) {

		return new ConstantNullReplacement(id);
	}

	private static CellGrid generateCellGrid(final N5Reader n5, final String dataset) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);

		final long[] dimensions = attributes.getDimensions();
		final int[] cellDimensions = attributes.getBlockSize();

		return new CellGrid(dimensions, cellDimensions);
	}

	@Override
	protected byte[] getData(final long... gridPosition) {

		final DataBlock<?> block;
		LOG.debug("Reading block for position {}", gridPosition);
		block = n5.readBlock(dataset, n5.getDatasetAttributes(dataset), gridPosition);
		LOG.debug("Read block for position {} {}", gridPosition, block);
		return block == null ? nullReplacement.apply(super.grid, gridPosition) : (byte[])block.getData();
	}

	private static class ConstantNullReplacement implements BiFunction<CellGrid, long[], byte[]> {

		private final long id;

		private ConstantNullReplacement(final long id) {

			this.id = id;
		}

		private static int numElements(final int[] size) {

			int n = 1;
			for (final int s : size)
				n *= s;
			return n;
		}

		// final ByteBuffer bb = ByteBuffer.wrap( bytes );
		//
		// final int argMaxSize = bb.getInt();
		// final long[] argMax = new long[ argMaxSize ];
		// for ( int i = 0; i < argMaxSize; ++i )
		// {
		// argMax[ i ] = bb.getLong();
		// }
		//
		// final int[] data = new int[ numElements ];
		// final int listDataSize = bytes.length - (
		// AbstractLabelMultisetLoader.listOffsetsSizeInBytes( data.length )
		// + AbstractLabelMultisetLoader.argMaxListSizeInBytes( argMax.length )
		// );
		// final LongMappedAccessData listData =
		// LongMappedAccessData.factory.createStorage( listDataSize );
		//
		// for ( int i = 0; i < data.length; ++i )
		// {
		// data[ i ] = bb.getInt();
		// }
		//
		// for ( int i = 0; i < listDataSize; ++i )
		// {
		// ByteUtils.putByte( bb.get(), listData.data, i );
		// }
		// return new VolatileLabelMultisetArray( data, listData, true, argMax
		// );
		@Override
		public byte[] apply(final CellGrid cellGrid, final long[] cellPos) {

			final long[] cellMin = new long[cellPos.length];
			final int[] cellDims = new int[cellMin.length];
			Arrays.setAll(cellMin, d -> cellPos[d] * cellGrid.cellDimension(d));
			cellGrid.getCellDimensions(cellPos, cellMin, cellDims);
			final int numElements = numElements(cellDims);

			final LongMappedAccessData listData = LongMappedAccessData.factory.createStorage(0);
			final LabelMultisetEntryList list = new LabelMultisetEntryList(listData, 0);
			final LabelMultisetEntry entry = new LabelMultisetEntry(0, 1);
			list.createListAt(listData, 0);
			entry.setId(id);
			entry.setCount(1);
			list.add(entry);
			final int listSize = (int)list.getSizeInBytes();

			final byte[] bytes = new byte[Integer.BYTES
					+ numElements * Long.BYTES // for argmaxes
					+ numElements * Integer.BYTES // for mappings
					+ listSize // for actual entries (one single entry)
			];

			final ByteBuffer bb = ByteBuffer.wrap(bytes);

			// argmax
			bb.putInt(numElements);
			for (int i = 0; i < numElements; ++i)
				bb.putLong(id);

			// offsets
			for (int i = 0; i < numElements; ++i)
				bb.putInt(0);

			// bb.putInt( 1 );
			LOG.debug("Putting id {}", id);
			for (int i = 0; i < listSize; ++i) {
				// ByteUtils.putByte( bb.get(), listData.data, i );
				bb.put(ByteUtils.getByte(listData.getData(), i));
			}
			// bb.putLong( id );
			// bb.putInt( 1 );

			LOG.debug("Returning {} bytes for {} elements", bytes.length, numElements);

			return bytes;
		}
	}
}
