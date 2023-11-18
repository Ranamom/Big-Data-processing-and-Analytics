package it.polito.bigdata.hadoop.lab;

import java.util.Vector;

/* This class is used to store the top-k elements of a set of objects of type T.
 * T is a class implementing the Comparable interface */

public class TopKVector<T extends Comparable<T>> {

	private Vector<WordCountWritable> localTopK;
	private Integer k;

	// It is used to create an empty TopKVector object.
	// k = number of top-k objects to store in this TopKVector object
	public TopKVector(int k) {
		this.localTopK = new Vector<WordCountWritable>();
		this.k = k;
	}

	public int getK() {
		return this.k;
	}

	// It is used to retrieve the vector containing the top-k objects among the
	// inserted ones
	public Vector<WordCountWritable> getLocalTopK() {
		return this.localTopK;
	}

	/*
	 * It is used to insert a new element in the current top-k vector. The new
	 * element is inserted in the this.localTopK vector if and only if it is in
	 * the top-k objects.
	 */
	public void updateWithNewElement(WordCountWritable currentElement) {

		if (localTopK.size() < k) { // There are less than k objects in
									// localTopK. Add the current element at the
									// end of localTopK
			// System.out.println("Dimensione localTopk: " + localTopK.size());
			localTopK.addElement(currentElement);
			// System.out.println("Ancora < 100 elem, inserisco: " + currentElement);

			// Sort the objects in localTopk
			sortAfterInsertNewElement();
		} else {
			// There are already k elements
			// Check if the current one is better than the least one
			// System.out.println("Dim array: " + k + " currentElem: " + currentElement + "
			// last elem in vector:"
			// + localTopK.elementAt(k - 1));
			if (currentElement.compareTo(localTopK.elementAt(k - 1)) > 0) {
				// The current element is better than the least object in
				// localTopK
				// Substitute the last object of localTopK with the current
				// object
				localTopK.setElementAt(currentElement, k - 1);
				// System.out.println("sopra 10 elem, inserisco: " + currentElement);
				// Sort the objects in localTopk
				sortAfterInsertNewElement();
			}
		}
	}

	private void sortAfterInsertNewElement() {
		// The last object is the only one that is potentially not in the right
		// position
		WordCountWritable swap;

		for (int pos = localTopK.size() - 1; pos > 0
				&& localTopK.elementAt(pos).compareTo(localTopK.elementAt(pos - 1)) > 0; pos--) {
			swap = localTopK.elementAt(pos);
			localTopK.setElementAt(localTopK.elementAt(pos - 1), pos);
			localTopK.setElementAt(swap, pos - 1);
		}
	}

}
