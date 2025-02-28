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

import net.imglib2.Cursor;
import net.imglib2.RandomAccessible;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.type.NativeType;
import net.imglib2.view.Views;

/**
 * A {@link CellLoader} that copies its data from a {@link RandomAccessible}.
 *
 * @deprecated This class belongs into imglib2-cache.
 *
 * @param <T>
 *            the type parameter
 *
 * @author Philipp Hanslovsky
 * @author Stephan Saalfeld
 */
@Deprecated
public class RandomAccessibleLoader<T extends NativeType<T>> implements CellLoader<T> {

	private final RandomAccessible<T> source;

	public RandomAccessibleLoader(final RandomAccessible<T> source) {

		super();
		this.source = source;
	}

	@Override
	public void load(final SingleCellArrayImg<T, ?> cell) {

		for (Cursor<T> s = Views.flatIterable(Views.interval(source, cell)).cursor(), t = cell.cursor(); s.hasNext();)
			t.next().set(s.next());
	}
}