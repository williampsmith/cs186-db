package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordID;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.table.RecordIterator;

import java.util.*;
import java.nio.file.Paths;

/**
 * A B+ tree. Allows the user to add, delete, search, and scan for keys in an
 * index. A BPlusTree has an associated page allocator. The first page in the
 * page allocator is a header page that serializes the search key data type,
 * root node page, and first leaf node page. Each subsequent page is a
 * BPlusNode, specifically either an InnerNode or LeafNode. Note that a
 * BPlusTree can have duplicate keys that appear across multiple pages.
 *
 * Properties:
 * allocator: PageAllocator for this index
 * keySchema: DataBox for this index's search key
 * rootPageNum: page number of the root node
 * firstLeafPageNum: page number of the first leaf node
 * numNodes: number of BPlusNodes
 */
public class BPlusTree {
    public static final String FILENAME_PREFIX = "db";
    public static final String FILENAME_EXTENSION = ".index";

    protected PageAllocator allocator;
    protected DataBox keySchema;
    private int rootPageNum;
    private int firstLeafPageNum;
    private int numNodes;

    /**
     * This constructor is used for creating an empty BPlusTree.
     *
     * @param keySchema the schema of the index key
     * @param fName the filename of where the index will be built
     */
    public BPlusTree(DataBox keySchema, String fName) {
        this(keySchema, fName, FILENAME_PREFIX);
    }

    public BPlusTree(DataBox keySchema, String fName, String filePrefix) {
        String pathname = Paths.get(filePrefix, fName + FILENAME_EXTENSION).toString();
        this.allocator = new PageAllocator(pathname, true);
        this.keySchema = keySchema;
        int headerPageNum = this.allocator.allocPage();
        assert(headerPageNum == 0);
        this.numNodes = 0;
        BPlusNode root = new LeafNode(this);
        this.rootPageNum = root.getPageNum();
        this.firstLeafPageNum = rootPageNum;
        writeHeader();
    }

    /**
     * This constructor is used for loading a BPlusTree from a file.
     *
     * @param fName the filename of a preexisting BPlusTree
     */
    public BPlusTree(String fName) {
        this(fName, FILENAME_PREFIX);
    }

    public BPlusTree(String fName, String filePrefix) {
        String pathname = Paths.get(filePrefix, fName + FILENAME_EXTENSION).toString();
        this.allocator = new PageAllocator(pathname, false);
        this.readHeader();
    }

    public void incrementNumNodes() {
        this.numNodes++;
    }

    public void decrementNumNodes() {
        this.numNodes--;
    }

    public int getNumNodes() {
        return this.numNodes;
    }

    /**
     * Perform a sorted scan.
     * The iterator should return all RecordIDs, starting from the beginning to
     * the end of the index.
     *
     * @return Iterator of all RecordIDs in sorted order
     */
    public Iterator<RecordID> sortedScan() {
        BPlusNode rootNode = BPlusNode.getBPlusNode(this, rootPageNum);
        return new BPlusIterator(rootNode);
    }

    /**
     * Perform a range search beginning from a specified key.
     * The iterator should return all RecordIDs, starting from the specified
     * key to the end of the index.
     *
     * @param keyStart the key to start iterating from
     * @return Iterator of RecordIDs that are equal to or greater than keyStart
     * in sorted order
     */
    public Iterator<RecordID> sortedScanFrom(DataBox keyStart) {
        BPlusNode root = BPlusNode.getBPlusNode(this, rootPageNum);
        return new BPlusIterator(root, keyStart, true);
    }

    /**
     * Perform an equality search on the specified key.
     * The iterator should return all RecordIDs that match the specified key.
     *
     * @param key the key to match
     * @return Iterator of RecordIDs that match the given key
     */
    public Iterator<RecordID> lookupKey(DataBox key) {
        BPlusNode root = BPlusNode.getBPlusNode(this, rootPageNum);
        return new BPlusIterator(root, key, false);
    }

    /**
     * Insert a (Key, RecordID) tuple.
     *
     * @param key the key to insert
     * @param rid the RecordID of the given key
     */
    public void insertKey(DataBox key, RecordID rid) {
        LeafEntry leafEntry = new LeafEntry(key, rid);
        BPlusNode root = BPlusNode.getBPlusNode(this, this.rootPageNum);
        BEntry returnedEntry = root.insertBEntry(leafEntry);
//        System.out.println(key);

        if (returnedEntry != null) {
            InnerNode newRoot = new InnerNode(this);
            newRoot.setFirstChild(this.rootPageNum);
            this.updateRoot(newRoot.getPageNum());
            List<BEntry> newEntry = new ArrayList<BEntry>();
            newEntry.add(returnedEntry);
            newRoot.overwriteBNodeEntries(newEntry);
        }
    }

    /**
     * Delete an entry with the matching key and RecordID.
     *
     * @param key the key to be deleted
     * @param rid the RecordID of the key to be deleted
     */
    public boolean deleteKey(DataBox key, RecordID rid) {
        /* You will not have to implement this in this project. */
        throw new BPlusTreeException("BPlusTree#DeleteKey Not Implemented!");
    }

    /**
     * Perform an equality search on the specified key.
     *
     * @param key the key to lookup
     * @return true if the key exists in this BPlusTree, false otherwise
     */
    public boolean containsKey(DataBox key) {
        return lookupKey(key).hasNext();
    }

    /**
     * Return the number of pages.
     *
     * @return the number of pages.
     */
    public int getNumPages() {
        return this.allocator.getNumPages();
    }

    /**
     * Update the root page.
     *
     * @param pNum the page number of the new root node
     */
    protected void updateRoot(int pNum) {
        this.rootPageNum = pNum;
        writeHeader();
    }

    private void writeHeader() {
        Page headerPage = allocator.fetchPage(0);
        int bytesWritten = 0;

        headerPage.writeInt(bytesWritten, this.rootPageNum);
        bytesWritten += 4;

        headerPage.writeInt(bytesWritten, this.firstLeafPageNum);
        bytesWritten += 4;

        headerPage.writeInt(bytesWritten, keySchema.type().ordinal());
        bytesWritten += 4;

        if (this.keySchema.type().equals(DataBox.Types.STRING)) {
            headerPage.writeInt(bytesWritten, this.keySchema.getSize());
            bytesWritten += 4;
        }
        headerPage.flush();
    }

    private void readHeader() {
        Page headerPage = allocator.fetchPage(0);

        int bytesRead = 0;

        this.rootPageNum = headerPage.readInt(bytesRead);
        bytesRead += 4;

        this.firstLeafPageNum = headerPage.readInt(bytesRead);
        bytesRead += 4;

        int keyOrd = headerPage.readInt(bytesRead);
        bytesRead += 4;
        DataBox.Types type = DataBox.Types.values()[keyOrd];

        switch(type) {
            case INT:
                this.keySchema = new IntDataBox();
                break;
            case STRING:
                int len = headerPage.readInt(bytesRead);
                this.keySchema = new StringDataBox(len);
                break;
            case BOOL:
                this.keySchema = new BoolDataBox();
                break;
            case FLOAT:
                this.keySchema = new FloatDataBox();
                break;
        }
    }

    /**
     * helper class for minimal storing of DFS data
     */
    public class NodeData {
        private int pageNum;
        private int index;

        public NodeData(int pageNum) {
            this.pageNum = pageNum;
            this.index = -1;
        }

        public void incrementIndex() {
            this.index++;
        }

        public void setIndex(int index) {
            this.index = index;
        }
    }


    /**
     * A BPlusIterator provides several ways of iterating over RecordIDs stored
     * in a BPlusTree.
     */
    private class BPlusIterator implements Iterator<RecordID> {
        private BPlusNode currentNode; // use int findChildFromKey(DataBox key) to get next node
        private List<BEntry> nodeEntries;
        private Iterator<RecordID> recordIter; // for leaf nodes
        private Stack<NodeData> nodeStack;
        private DataBox key;
        private boolean scan;
        private boolean fullScan;
        private boolean notFound; // flag set if constructor finds that search will return empty

        /**
         * Helper function
         * Begin a DFS to find the first leaf node while saving state
         * @param root root node of tree
         */
        public void initializeRange(BPlusNode root) {
            if (root.isLeaf()) {
                return;
            }

//            this.nodeStack.push(new NodeData(root.getPageNum()));
            BPlusNode current = root;

            while (!current.isLeaf()) {
                nodeStack.push(new NodeData(current.getPageNum()));
                List<BEntry> currentNodeEntries = current.getAllValidEntries();
                int childPage = ((InnerNode)current).getFirstChild();
                current = BPlusNode.getBPlusNode(BPlusTree.this, childPage);
            }
            this.currentNode = current;
        }

        public void initializeRangeBeginningAt(DataBox key, BPlusNode root) {
            if (root.isLeaf()) {
                return;
            }

            //push the root
            BPlusNode current = root;
            NodeData nodeData = new NodeData(root.getPageNum());

            while (!current.isLeaf()) {
                int childPage = ((InnerNode)current).findChildFromKey(key, nodeData);
                this.nodeStack.push(nodeData);
                current = BPlusNode.getBPlusNode(BPlusTree.this, childPage);
                nodeData = new NodeData(current.getPageNum());
            }
            // current node is now a leaf
            this.currentNode = current;

            if (this.scan) {
                while (!((LeafNode)current).scanFrom(key).hasNext()) {
                    current = this.getNextLeaf();
                    if (current == null) {
                        this.notFound = true;
                        break;
                    }
                }
            } else {
//                System.out.println((((LeafNode)current).containsKey(key)));
                while (!(((LeafNode)current).containsKey(key))) {
                    current = this.getNextLeaf();
//                    System.out.println(current.getAllValidEntries());
                    if (current == null) {
                        this.notFound = true;
                        break;
                    }

                    if (key.compareTo(current.getAllValidEntries().get(0).getKey()) == -1) {
                        this.notFound = true;
                        break;
                    }
                }
            }
            this.currentNode = current;
//            System.out.println(current.getAllValidEntries());
        }

        /**
         * Helper function
         * Continues DFS from constructor by traversing to next leaf node and updating the stack.
         * Updates currentNode. After execution, currentNode will be a leaf node, or null.
         */
        public BPlusNode getNextLeaf() {
            if (this.nodeStack.empty()){
//                System.out.println("1");
                return null;
            }

            if (BPlusTree.this.rootPageNum == this.currentNode.getPageNum()) {
//                System.out.println("2");
                return null;
            }

            NodeData currentData = this.nodeStack.peek();
            currentData.incrementIndex();
            BPlusNode current = BPlusNode.getBPlusNode(BPlusTree.this, currentData.pageNum);
            List<BEntry> currentEntries = current.getAllValidEntries();

            // find first parent node with unvisited children
            while (currentData.index >= currentEntries.size()) { // visited all children
                    this.nodeStack.pop();

                    if (this.nodeStack.empty()) {
                        this.notFound = true;
//                        System.out.println("3");
                        return null;
                    }
                    currentData = this.nodeStack.peek();
                    currentData.incrementIndex();
                    current = BPlusNode.getBPlusNode(BPlusTree.this, currentData.pageNum);
                    currentEntries = current.getAllValidEntries();
            }

            // get first child
            BEntry childEntry = currentEntries.get(currentData.index);
            NodeData childData = new NodeData(childEntry.getPageNum());
            BPlusNode child = BPlusNode.getBPlusNode(BPlusTree.this, childData.pageNum);

            // find first unvisited leaf node
            while (!child.isLeaf()) { // traverse children until we reach leaf
                this.nodeStack.push(childData);
                childData = new NodeData(((InnerNode)child).getFirstChild());
                child = BPlusNode.getBPlusNode(BPlusTree.this, childData.pageNum);
            }

            // now child is an unvisited leaf
            this.currentNode = child;
            this.nodeEntries = this.currentNode.getAllValidEntries();
            return child;
        }

        /**
         * Construct an iterator that performs a sorted scan on this BPlusTree
         * tree.
         * The iterator should return all RecordIDs, starting from the
         * beginning to the end of the index.
         *
         * @param root the root node of this BPlusTree
         */
        public BPlusIterator(BPlusNode root) {
            this.currentNode = root;
            this.nodeEntries = this.currentNode.getAllValidEntries();
            this.recordIter = null; // set once we reach leaf
            this.nodeStack = new Stack<NodeData>();
            this.key = null;
            this.scan = true;
            this.fullScan = true;
            this.notFound = false;

            if (root == null) {
                throw new NullPointerException("Error: Root is null");
            }

            this.initializeRange(root);
            this.recordIter = ((LeafNode) this.currentNode).scan();
            if (!this.recordIter.hasNext()) {
                this.notFound = true;
            }
        }

        /**
         * Construct an iterator that performs either an equality or range
         * search with a specified key.
         * If @param scan is true, the iterator should return all RecordIDs,
         * starting from the specified key to the end of the index.
         * If @param scan is false, the iterator should return all RecordIDs
         * that match the specified key.
         *
         * @param root the root node of this BPlusTree
         * @param key the specified key value
         * @param scan if true, do a range search; else, equality search
         */
        public BPlusIterator(BPlusNode root, DataBox key, boolean scan) {
            this.currentNode = root;
            this.nodeEntries = this.currentNode.getAllValidEntries();
            this.recordIter = null; // set once we reach leaf
            this.nodeStack = new Stack<NodeData>();
            this.key = key;
            this.scan = scan;
            this.fullScan = false;
            this.notFound = false;

            if(root == null) {
                throw new BPlusTreeException("Error: Root is null");
            }

            if (key == null) {
                throw new BPlusTreeException("Error: Key is null");
            }

            this.initializeRangeBeginningAt(key, root);

            if (!this.notFound) {
                if (scan) { // seek to beginning of scan
                    this.recordIter = ((LeafNode) this.currentNode).scanFrom(key);
                } else { // seek to first equality
                    this.recordIter = ((LeafNode) this.currentNode).scanForKey(key);
                }

                if (!this.recordIter.hasNext()) {
                    this.notFound = true;
                }
            }
        }

        /**
         * Confirm if iterator has more RecordIDs to return.
         *
         * @return true if there are still RecordIDs to be returned, false
         * otherwise
         */
        public boolean hasNext() {
//            System.out.println("hasNext: " + (!this.notFound && this.recordIter.hasNext()));
            return !this.notFound && this.recordIter.hasNext();
        }

        /**
         * Yield the next RecordID of this iterator.
         *
         * @return the next RecordID
         * @throws NoSuchElementException if there are no more RecordIDs to
         * yield
         */
        public RecordID next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException("Error: key not found in index");
            }

            RecordID nextRecord = this.recordIter.next();
            if (!this.recordIter.hasNext()) { // go to next node
                BPlusNode nextNode = this.getNextLeaf();
                this.currentNode = nextNode;
//                System.out.println(this.currentNode.getAllValidEntries());

                if (nextNode == null) {
                    this.notFound = true; // next call to hasNext() will return false
                }
                else if (this.fullScan) {
                    this.recordIter = ((LeafNode) this.currentNode).scan();
                }
                else if (this.scan) {
                    this.recordIter = ((LeafNode) this.currentNode).scanFrom(key);
                } else { // seek to first equality
                    this.recordIter = ((LeafNode) this.currentNode).scanForKey(key);
                }
            }

            if (nextRecord == null) { // should never happen
                throw new NoSuchElementException("Error. Something went wrong in next().");
            }

            return nextRecord;
        }

        public void remove() {
            /* You will not have to implement this in this project. */
            throw new UnsupportedOperationException();
        }
    }
}
