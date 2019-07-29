/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.common.collections;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class Lists2 {

    private Lists2() {
    }

    /**
     * Create a new list that contains the elements of both arguments
     */
    public static <T> List<T> concat(Collection<? extends T> list1, Collection<? extends T> list2) {
        ArrayList<T> list = new ArrayList<>(list1.size() + list2.size());
        list.addAll(list1);
        list.addAll(list2);
        return list;
    }

    public static <T> List<T> concat(Collection<? extends T> list1, T item) {
        ArrayList<T> xs = new ArrayList<>(list1.size() + 1);
        xs.addAll(list1);
        xs.add(item);
        return xs;
    }

    public static <T> List<T> concatUnique(List<? extends T> list1, List<? extends T> list2) {
        List<T> result = new ArrayList<>(list1.size() + list2.size());
        result.addAll(list1);
        for (T item : list2) {
            if (!list1.contains(item)) {
                result.add(item);
            }
        }
        return result;
    }

    /**
     * Change the layout from rows to column oriented.
     *
     * <pre>
     *
     * rows: [
     *  [a, 1],
     *  [b, 2]
     * ]
     *
     * to
     *
     * columnValues: [
     *  [a, b],
     *  [1, 2]
     * ]
     * </pre>
     */
    public static <T> List<List<T>> toColumnOrientation(List<? extends List<T>> rows) {
        if (rows.isEmpty()) {
            return List.of();
        }
        List<T> firstRow = rows.get(0);
        int numColumns = firstRow.size();
        ArrayList<List<T>> columns = new ArrayList<>();
        for (int c = 0; c < numColumns; c++) {
            ArrayList<T> columnValues = new ArrayList<>(rows.size());
            for (int r = 0; r < rows.size(); r++) {
                List<T> row = rows.get(r);
                columnValues.add(row.get(c));
            }
            columns.add(columnValues);
        }
        return columns;
    }

    /**
     * Apply the replace function on each item of the list and replaces the item.
     *
     * This is similar to Guava's com.google.common.collect.Lists#transform(List, com.google.common.base.Function),
     * but instead of creating a view on a backing list this function is actually mutating the provided list
     */
    public static <T> void mutate(@Nullable List<T> list, Function<? super T, ? extends T> mapper) {
        if (list == null || list.isEmpty()) {
            return;
        }
        ListIterator<T> it = list.listIterator();
        while (it.hasNext()) {
            it.set(mapper.apply(it.next()));
        }
    }

    /**
     * Create a copy of the given list with {@code mapper} applied on each item.
     * Opposed to {@link java.util.stream.Stream#map(Function)} / {@link Collectors#toList()} this minimizes allocations.
     */
    public static <I, O> List<O> map(Collection<I> list, Function<? super I, ? extends O> mapper) {
        List<O> copy = new ArrayList<>(list.size());
        for (I item : list) {
            copy.add(mapper.apply(item));
        }
        return copy;
    }

    /**
     * Return the first element of a list or raise an IllegalArgumentException if there are more than 1 items.
     *
     * Similar to Guava's com.google.common.collect.Iterables#getOnlyElement(Iterable), but avoids an iterator allocation
     *
     * @throws NoSuchElementException If the list is empty
     * @throws IllegalArgumentException If the list has more than 1 element
     */
    public static <T> T getOnlyElement(List<T> items) {
        switch (items.size()) {
            case 0:
                throw new NoSuchElementException("List is empty");

            case 1:
                return items.get(0);

            default:
                throw new IllegalArgumentException("Expected 1 element, got: " + items.size());
        }
    }

    public static <O, I> List<O> mapTail(O head, List<I> tail, Function<I, O> mapper) {
        ArrayList<O> list = new ArrayList<>(tail.size() + 1);
        list.add(head);
        for (I input : tail) {
            list.add(mapper.apply(input));
        }
        return list;
    }

    /**
     * Finds the first non peer element in the provided list of items between the begin and end indexes.
     * Two items are peers if the provided comparator designates them as equals.
     * @return the position of the first item that's not equal with the item on the `begin` index in the list of items.
     */
    public static <T> int findFirstNonPeer(List<T> items, int begin, int end, @Nullable Comparator<T> cmp) {
        if (cmp == null || (begin + 1) >= end) {
            return end;
        }
        T fst = items.get(begin);
        if (cmp.compare(fst, items.get(begin + 1)) != 0) {
            return begin + 1;
        }
        /*
         * Adapted binarySearch algorithm to find the first non peer (instead of the first match)
         * This depends on there being at least some EQ values;
         * Whenever we find a EQ pair we check if the following element isn't EQ anymore.
         *
         * E.g.
         *
         * i:     0  1  2  3  4  5  6  7
         * rows: [1, 1, 1, 1, 4, 4, 5, 6]
         *        ^ [1  1  1  4  4  5  6]
         *        +-----------^
         *           cmp: -1
         *        1 [1  1  1  4] 4  5  6
         *        ^     ^
         *        +-----+
         *           cmp: 0 --> cmp (mid +1) != 0 --> false
         *        1  1  1 [1  4] 4  5  6
         *        ^        ^
         *        +--------+
         *           cmp: 0 --> cmp (mid +1) != 0 --> true
         */
        int low = begin + 1;
        int high = end;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            T t = items.get(mid);
            int cmpResult = cmp.compare(fst, t);
            if (cmpResult == 0) {
                int next = mid + 1;
                if (next == high || cmp.compare(fst, items.get(next)) != 0) {
                    return next;
                } else {
                    low = next;
                }
            } else if (cmpResult < 0) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return end;
    }

    /**
     * Finds the first peer, in order of appearance in the items list, of the item at the given index.
     * If the provided comparator is null this will return 0 (all items are peers when no comparator is specified).
     * If the provided item has no peers amongst the items that appear before it, or if it is the first item in the
     * list, this will return the itemIdx.
     */
    public static <T> int findFirstPreviousPeer(List<T> items, int itemIdx, @Nullable Comparator<T> cmp) {
        if (cmp == null) {
            return 0;
        }

        int firstPeer = itemIdx;
        T item = items.get(itemIdx);
        for (int i = itemIdx - 1; i >= 0; i--) {
            if (cmp.compare(item, items.get(i)) == 0) {
                firstPeer = i;
            } else {
                break;
            }
        }
        return firstPeer;
    }

    /**
     * Finds the first item that's less than or equal to the probe in the slice of the sortedItems that starts with the index
     * specified by @param itemIdx, according to the provided comparator.
     * @return the index of the first LTE item, or -1 if there isn't any (eg. probe is less than all items)
     */
    public static <T> int findFirstLTEProbeValue(List<T> sortedItems, int itemIdx, T probe, Comparator<T> cmp) {
        int start = itemIdx;
        int end = sortedItems.size() - 1;

        int firstLTEProbeIdx = -1;
        while (start <= end) {
            int mid = (start + end) >>> 1;
            // Move to left side if mid is greater than probe
            if (cmp.compare(sortedItems.get(mid), probe) > 0) {
                end = mid - 1;
            } else {
                firstLTEProbeIdx = mid;
                start = mid + 1;
            }
        }
        return firstLTEProbeIdx;
    }

    /**
     * Finds the first item that's greater than or equal to the probe in the slice of the sortedItems that ends with the index
     * specified by @param itemIdx, according to the provided comparator.
     * @return the index of the first GTE item, or -1 if there isn't any (eg. probe is greater than all items)
     */
    public static <T> int findFirstGTEProbeValue(List<T> sortedItems, int itemIdx, T probe, Comparator<T> cmp) {
        int start = 0;
        int end = itemIdx - 1;

        int firstGTEProbeIdx = -1;
        while (start <= end) {
            int mid = (start + end) >>> 1;
            // Move to right side if mid is less than probe
            if (cmp.compare(sortedItems.get(mid), probe) < 0) {
                start = mid + 1;
            } else {
                firstGTEProbeIdx = mid;
                end = mid - 1;
            }
        }
        return firstGTEProbeIdx;
    }

    /**
     * Indicates if the items at pos1 and pos2 are equal (ie. peers)  with respect to the provided comparator.
     * @return true if the comparator is null, or true/false if the comparator designates the two items as true or false.
     */
    public static <T> boolean arePeers(List<T> items, int pos1, int pos2, @Nullable Comparator<T> cmp) {
        if (cmp == null) {
            return true;
        }
        T fst = items.get(pos1);
        return cmp.compare(fst, items.get(pos2)) == 0;
    }

}