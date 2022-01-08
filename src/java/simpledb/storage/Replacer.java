package simpledb.storage;

public interface Replacer {

    PageId victim();

    void add(PageId pageId);

}
