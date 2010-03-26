package cascading.clojure;

import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.OperationCall;
import cascading.operation.BufferCall;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import clojure.lang.IFn;
import clojure.lang.AFn;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import clojure.lang.RT;
import java.util.Iterator;
import java.util.Collection;


public class ClojureBuffer extends BaseOperation<Object>
                           implements Buffer<Object> {
  private Object[] fn_spec;
  private IFn fn;

  protected static class TupleSeqConverter implements Iterator<ISeq> {
    private Iterator<TupleEntry> tuples;

    public TupleSeqConverter(Iterator<TupleEntry> tuples) {
      this.tuples = tuples;
    }

    public boolean hasNext() {
      return this.tuples.hasNext();
    }

    public ISeq next() {
      return Util.coerceFromTuple(this.tuples.next());
    }

    public void remove() {
      this.tuples.remove();
    }
  }

  protected static class CollectFn extends AFn implements IFn {
    private TupleEntryCollector collector;

    public CollectFn(TupleEntryCollector collector) {
      this.collector = collector;
    }

    public Object invoke(Object obj) throws Exception {
      this.collector.add(Util.coerceToTuple(obj));
      return null;
    }
  }

  public ClojureBuffer(Fields fn_fields, Collection fn_spec) {
    super(fn_fields);
    this.fn_spec = fn_spec.toArray();
  }

  public void prepare(FlowProcess flow_process, OperationCall<Object> op_call) {
    this.fn = Util.bootFn(fn_spec);
  }

  public void operate(FlowProcess flow_process, BufferCall<Object> buff_call) {
    try {
      TupleSeqConverter in_iter = new TupleSeqConverter(buff_call.getArgumentsIterator());
      IFn collect_fn = new CollectFn(buff_call.getOutputCollector());
      this.fn.invoke(in_iter, collect_fn);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
