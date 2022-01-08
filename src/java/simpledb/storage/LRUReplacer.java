package simpledb.storage;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class LRUReplacer implements Replacer{


    private HashMap<PageId, Node> map;
    private int size;
    private Node head;
    private Node tail;

    private ReentrantLock lock = new ReentrantLock();


    class Node {
        Node next, prev;
        PageId pid;
        Node (PageId pid) {
            this.pid = pid;
        }
    }

    private void remove(Node node) {
        Node prev = node.prev;
        Node next = node.next;
        prev.next = next;
        next.prev = prev;
    }

    public LRUReplacer(int size) {
        this.size = size;
        head = new Node(null);
        tail = new Node(null);
        head.next = tail;
        tail.prev = head;
        map = new HashMap<>();
    }


    @Override
    public PageId victim() {
        try {
            lock.lock();
            if (map.size() < size) {
                return null;
            }
            Node victimed = removeLast();
            map.remove(victimed.pid);

            return victimed.pid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void add(PageId pageId) {
        try {
            lock.lock();
            if (map.containsKey(pageId)) {
                Node node = map.get(pageId);
                remove(node);
                put2Head(node);
            } else {
                if (map.size() >= size) {
                    Node removeNode = removeLast();
                    map.remove(removeNode);
                }
                Node newNode = new Node(pageId);
                put2Head(newNode);
                map.put(pageId, newNode);
            }
        } finally {
            lock.unlock();
        }
    }

    private Node removeLast() {
        Node removedNode = tail.prev;
        tail.prev = removedNode.prev;
        removedNode.prev.next = tail;
        return removedNode;
    }


    private void put2Head(Node node) {
        node.next = head.next;
        head.next.prev = node;
        node.prev = head;
    }
}
