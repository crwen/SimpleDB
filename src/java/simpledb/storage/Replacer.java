package simpledb.storage;

public interface Replacer {

    PageId victim();

    void add(PageId pageId);

    void pin(PageId pageId);
    void unpin(PageId pageId);

}
