package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
    	return new Iterator<TDItem>() {
        	private int i;
        	
        	@Override
        	public boolean hasNext() {
        		return i < td.length;
        	}
        	
        	@Override
        	public TDItem next() {
        		if (!hasNext()) throw new NoSuchElementException();
        		i ++;
        		return td[i];
        	}
        };
    }

    private static final long serialVersionUID = 1L;
    private TDItem[] td;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	td = new TDItem[typeAr.length];
    	for (int i = 0; i < typeAr.length; i++) {
			td[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    }

    public TupleDesc GenerateAlias(String tableAlias) {
    	TDItem[] res = new TDItem[td.length];
    	for (int i = 0; i < td.length; i++) {
    		res[i] = new TDItem(td[i].fieldType, tableAlias + "." + td[i].fieldName);
    	}
    	return new TupleDesc(res);
    }
    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	this(typeAr, new String[typeAr.length]);
    }
    
    public TupleDesc(TDItem[] td) {
    	this.td = td;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return td.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if (i >= td.length || i < 0) {
    		throw new NoSuchElementException();
    	}
        return td[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if (i >= td.length || i < 0) {
    		throw new NoSuchElementException();
    	}
        return td[i].fieldType;
    }

    public TDItem getith(int i) {
    	return td[i];
    }
    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	if (name == null) {
    		throw new NoSuchElementException();
    	}
        for (int i = 0; i < td.length; i++) {
        	if (Objects.equals(td[i].fieldName, name)) {
        		return i;
        	}
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int res = 0;
        for (TDItem i: td) {
        	res += i.fieldType.getLen();
        }
        return res;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	TDItem[] res = new TDItem[td1.numFields()+td2.numFields()];
    	for (int i = 0; i < td1.numFields(); i++) {
    		res[i] = td1.getith(i);
    	}
    	for (int i = 0; i < td2.numFields(); i++) {
    		res[td1.numFields()+i] = td2.getith(i);
    	}
    	return new TupleDesc(res);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
    	if (o == null || !(o instanceof TupleDesc)) {
    		return false;
    	}
        TupleDesc other = (TupleDesc)o;
        if (numFields() != other.numFields()) {
        	return false;
        }
        for (int i = 0; i < numFields(); i++) {
        	if (!(Objects.equals(other.getFieldName(0), getFieldName(0)) &&
        			other.getFieldType(i).getLen() == getFieldType(i).getLen())) {
        		return false;
        	}
        	
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuffer sb = new StringBuffer();
        for (TDItem i: td) {
        	sb.append(i.fieldType);
        	sb.append("(");
        	if (i.fieldName != null) {
        		sb.append(i.fieldName);
        	}
        	sb.append(")");
        }
        return sb.toString();
    }
}
