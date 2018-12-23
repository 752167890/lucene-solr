/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs.lucene50;


import static org.apache.lucene.codecs.lucene50.ForUtil.MAX_DATA_SIZE;
import static org.apache.lucene.codecs.lucene50.ForUtil.MAX_ENCODED_SIZE;
import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.DOC_CODEC;
import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.MAX_SKIP_LEVELS;
import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.PAY_CODEC;
import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.POS_CODEC;
import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.TERMS_CODEC;
import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.VERSION_CURRENT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Concrete class that writes docId(maybe frq,pos,offset,payloads) list
 * with postings format.
 *
 * Postings list for each term will be stored separately. 
 *
 * @see Lucene50SkipWriter for details about skipping setting and postings layout.
 * @lucene.experimental
 */
public final class Lucene50PostingsWriter extends PushPostingsWriterBase {

  IndexOutput docOut;
  IndexOutput posOut;
  IndexOutput payOut;

  final static IntBlockTermState emptyState = new IntBlockTermState();
  IntBlockTermState lastState;

  // Holds starting file pointers for current term:
  private long docStartFP;
  private long posStartFP;
  private long payStartFP;

  final ArrayList<Integer> docIdBuffer;
  final int[] freqBuffer;
  private int docBufferUpto;

  final int[] posDeltaBuffer;
  final int[] payloadLengthBuffer;
  final int[] offsetStartDeltaBuffer;
  final int[] offsetLengthBuffer;
  private int posBufferUpto;

  private byte[] payloadBytes;
  private int payloadByteUpto;

  private int lastBlockDocID;
  private long lastBlockPosFP;
  private long lastBlockPayFP;
  private int lastBlockPosBufferUpto;
  private int lastBlockPayloadByteUpto;

  private int baseDocID;
  private int lastPosition;
  private int lastStartOffset;
  private int docCount;

  final byte[] encoded;

  private final ForUtil forUtil;
  private final Lucene50SkipWriter skipWriter;

  enum EncodeMethod {
    BitSet, VByte
  }
  public class Partition {
    public int  Start;
    public int  End;
    Partition(int start, int end) {
      Start = start;
      End = end;
    }
  }
  public class PartitionItem {
    public Partition  Partition;
    public EncodeMethod Method;
    PartitionItem(int start, int end, EncodeMethod method) {
      Partition = new Partition(start, end);
      Method = method;
    }
  }
  /** Creates a postings writer */
  public Lucene50PostingsWriter(SegmentWriteState state) throws IOException {
    final float acceptableOverheadRatio = PackedInts.COMPACT;

    String docFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene50PostingsFormat.DOC_EXTENSION);
    docOut = state.directory.createOutput(docFileName, state.context);
    IndexOutput posOut = null;
    IndexOutput payOut = null;
    boolean success = false;
    try {
      CodecUtil.writeIndexHeader(docOut, DOC_CODEC, VERSION_CURRENT, 
                                   state.segmentInfo.getId(), state.segmentSuffix);
      forUtil = new ForUtil(acceptableOverheadRatio, docOut);
      if (state.fieldInfos.hasProx()) {
        posDeltaBuffer = new int[MAX_DATA_SIZE];
        String posFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene50PostingsFormat.POS_EXTENSION);
        posOut = state.directory.createOutput(posFileName, state.context);
        CodecUtil.writeIndexHeader(posOut, POS_CODEC, VERSION_CURRENT,
                                     state.segmentInfo.getId(), state.segmentSuffix);

        if (state.fieldInfos.hasPayloads()) {
          payloadBytes = new byte[128];
          payloadLengthBuffer = new int[MAX_DATA_SIZE];
        } else {
          payloadBytes = null;
          payloadLengthBuffer = null;
        }

        if (state.fieldInfos.hasOffsets()) {
          offsetStartDeltaBuffer = new int[MAX_DATA_SIZE];
          offsetLengthBuffer = new int[MAX_DATA_SIZE];
        } else {
          offsetStartDeltaBuffer = null;
          offsetLengthBuffer = null;
        }

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          String payFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene50PostingsFormat.PAY_EXTENSION);
          payOut = state.directory.createOutput(payFileName, state.context);
          CodecUtil.writeIndexHeader(payOut, PAY_CODEC, VERSION_CURRENT,
                                       state.segmentInfo.getId(), state.segmentSuffix);
        }
      } else {
        posDeltaBuffer = null;
        payloadLengthBuffer = null;
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        payloadBytes = null;
      }
      this.payOut = payOut;
      this.posOut = posOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
    }

    docIdBuffer = new ArrayList<>();
//    freqBuffer = new int[MAX_DATA_SIZE];
    freqBuffer = null;

    // TODO: should we try skipping every 2/4 blocks...?
    skipWriter = new Lucene50SkipWriter(MAX_SKIP_LEVELS,
                                        BLOCK_SIZE, 
                                        state.segmentInfo.maxDoc(),
                                        docOut,
                                        posOut,
                                        payOut);

//    encoded = new byte[MAX_ENCODED_SIZE];
    encoded = null;
  }

  @Override
  public IntBlockTermState newTermState() {
    return new IntBlockTermState();
  }

  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    CodecUtil.writeIndexHeader(termsOut, TERMS_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
    termsOut.writeVInt(BLOCK_SIZE);
  }

  @Override
  public int setField(FieldInfo fieldInfo) {
    super.setField(fieldInfo);
    skipWriter.setField(writePositions, writeOffsets, writePayloads);
    lastState = emptyState;
    if (writePositions) {
      if (writePayloads || writeOffsets) {
        return 3;  // doc + pos + pay FP
      } else {
        return 2;  // doc + pos FP
      }
    } else {
      return 1;    // doc FP
    }
  }

  @Override
  public void startTerm() {
    docStartFP = docOut.getFilePointer();
    if (writePositions) {
      posStartFP = posOut.getFilePointer();
      if (writePayloads || writeOffsets) {
        payStartFP = payOut.getFilePointer();
      }
    }
    lastBlockDocID = -1;
    skipWriter.resetSkip();
  }

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
//    // 所有数据暂时缓存到内存中，直到finishTerm再处理
//    if (lastBlockDocID != -1 && docBufferUpto == 0) {
//      skipWriter.bufferSkip(lastBlockDocID, docCount, lastBlockPosFP, lastBlockPayFP, lastBlockPosBufferUpto, lastBlockPayloadByteUpto);
//    }
    // 如果数据有误
    if (docID < 0) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + baseDocID + " )", docOut);
    }
    docIdBuffer.add(docID);
    if (writeFreqs) {
      freqBuffer[docBufferUpto] = termDocFreq;
    }
    docBufferUpto++;
    docCount++;
    // 先缓存最后处理
//    if (docBufferUpto == BLOCK_SIZE) {
//      // forUtil.writeBlock(docOffsetBuffer, encoded, docOut);
//      if (writeFreqs) {
//        forUtil.writeBlock(freqBuffer, encoded, docOut);
//      }
//      // NOTE: don't set docBufferUpto back to 0 here;
//      // finishDoc will do so (because it needs to see that
//      // the block was filled so it can save skip data)
//    }

    lastPosition = 0;
    lastStartOffset = 0;
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
    if (position > IndexWriter.MAX_POSITION) {
      throw new CorruptIndexException("position=" + position + " is too large (> IndexWriter.MAX_POSITION=" + IndexWriter.MAX_POSITION + ")", docOut);
    }
    if (position < 0) {
      throw new CorruptIndexException("position=" + position + " is < 0", docOut);
    }
    posDeltaBuffer[posBufferUpto] = position - lastPosition;
    if (writePayloads) {
      if (payload == null || payload.length == 0) {
        // no payload
        payloadLengthBuffer[posBufferUpto] = 0;
      } else {
        payloadLengthBuffer[posBufferUpto] = payload.length;
        if (payloadByteUpto + payload.length > payloadBytes.length) {
          payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payload.length);
        }
        System.arraycopy(payload.bytes, payload.offset, payloadBytes, payloadByteUpto, payload.length);
        payloadByteUpto += payload.length;
      }
    }

    if (writeOffsets) {
      assert startOffset >= lastStartOffset;
      assert endOffset >= startOffset;
      offsetStartDeltaBuffer[posBufferUpto] = startOffset - lastStartOffset;
      offsetLengthBuffer[posBufferUpto] = endOffset - startOffset;
      lastStartOffset = startOffset;
    }
    
    posBufferUpto++;
    lastPosition = position;
    if (posBufferUpto == BLOCK_SIZE) {
      forUtil.writeBlock(posDeltaBuffer, encoded, posOut);

      if (writePayloads) {
        forUtil.writeBlock(payloadLengthBuffer, encoded, payOut);
        payOut.writeVInt(payloadByteUpto);
        payOut.writeBytes(payloadBytes, 0, payloadByteUpto);
        payloadByteUpto = 0;
      }
      if (writeOffsets) {
        forUtil.writeBlock(offsetStartDeltaBuffer, encoded, payOut);
        forUtil.writeBlock(offsetLengthBuffer, encoded, payOut);
      }
      posBufferUpto = 0;
    }
  }

  @Override
  public void finishDoc() throws IOException {
    // Since we don't know df for current term, we had to buffer
    // those skip data for each block, and when a new doc comes, 
    // write them to skip file.
//    if (docBufferUpto == BLOCK_SIZE) {
//      if (posOut != null) {
//        if (payOut != null) {
//          lastBlockPayFP = payOut.getFilePointer();
//        }
//        lastBlockPosFP = posOut.getFilePointer();
//        lastBlockPosBufferUpto = posBufferUpto;
//        lastBlockPayloadByteUpto = payloadByteUpto;
//      }
//      docBufferUpto = 0;
//    }
  }
  private int getVIntCost(int val) {
    if (val < 0) {
      return 0;
    }
    int cost = 1;
    while ((val & ~0x7F) != 0) {
      val >>>= 7;
      cost += 1;
    }
    return cost;
  }

  private int getBitSetCost(int universe) {
    // 得到使用的byte数
    if (universe < 0) {
      return 0;
    }
    return ((universe - 1) >> 3) + 1;
  }


  public ArrayList<PartitionItem> optimalPartition() throws IOException {
    ArrayList<PartitionItem> partition = new ArrayList<>();
    int min = 0, max = 0, i = 0, j = 0, g = 0;
    int baseDocId = docIdBuffer.get(0);
    int extra = 3 * getVIntCost(baseDocId);
    int T = extra;
    int lastPos = 0;
    for (int index = 1; index < docIdBuffer.size(); index++) {
      int E = getVIntCost(docIdBuffer.get(index) - baseDocId - 1);
      int x = getBitSetCost(docIdBuffer.get(index) - baseDocId - 1);
      int y = getBitSetCost(docIdBuffer.get(index - 1) - baseDocId - 1);
      int B = x-y;
      int flag = E - B;
      g += flag;
      if (flag >= 0) {
        if (g > max) {
          max = g;
          i = index + 1;
        }
        if (min < -T && min - g < -2 * extra) {
          partition.add(new PartitionItem(lastPos, j, EncodeMethod.VByte));
          i = j + 1;
          g = g - min;
          min = 0;
          max = 0;
          lastPos = j;
          // 处理跳表及baseDocId的问题
          baseDocId = docIdBuffer.get(j);
          // 开始回溯
          index = j+1;
          extra = 2 * getVIntCost(baseDocId);
          T = 2 * extra;
        }
      } else {
        if (g < min) {
          min = g;
          j = index + 1;
        }
        if (max < -T && max - g < -2 * extra) {
          partition.add(new PartitionItem(lastPos, i, EncodeMethod.BitSet));
          j = i + 1;
          g = g - max;
          max = 0;
          min = 0;
          // 处理跳表及baseDocId的问题
          lastPos = i;
          baseDocId = docIdBuffer.get(i);
          // 开始回溯
          index = i+1;
          extra = 2 * getVIntCost(baseDocId);
          T = 2 * extra;
        }
      }
    }
//    if (max > extra && max - g > extra) {
//      partition.add(i);
//      g = g - max;
//    }
//    if (min < -extra && min - g < -extra) {
//      partition.add(j);
//      g = g - min;
//    }
    if (g > 0) {
      partition.add(new PartitionItem(lastPos, docIdBuffer.size(), EncodeMethod.BitSet));
    } else {
      partition.add(new PartitionItem(lastPos, docIdBuffer.size(), EncodeMethod.VByte));
    }
    return partition;
  }

  private byte[] longsToBytes(long[] x) {
    List<Byte> baos = new ArrayList<>();
    for(int k=0; k<x.length; k++){
      long data = x[k];
      byte[] buffer = new byte[8];
      for (int i = 0; i < 8; i++) {
        int offset = 64 - (i + 1) * 8;
        buffer[i] = (byte) ((data >> offset) & 0xff);
      }
      // 对于最后一个long特殊处理，去掉高位无用的0
      if (k==x.length-1) {
        int j=0;
        while(buffer[j]==0) {
          j++;
          if (j==8) {
            break;
          }
        }
        for (int l=7;l>=j;l--) {
          baos.add(buffer[l]);
        }
      } else {
        for(int j=7; j>=0; j--) {
          baos.add(buffer[j]);
        }
      }
    }
    byte[] res=new byte[baos.size()];
    for(int i=0;i<baos.size();i++) {
      res[i]=baos.get(i);
    }
    return res;
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    assert state.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert state.docFreq == docCount: state.docFreq + " vs " + docCount;
    
    // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer to it.
    final int singletonDocID;
    if (state.docFreq == 1) {
      // pulse the singleton docid into the term dictionary, freq is implicitly totalTermFreq
      singletonDocID = docIdBuffer.get(0);
    } else {
      singletonDocID = -1;
      // 遍历所有的docId，得到最优的分块
      ArrayList<PartitionItem> partition = optimalPartition();
      for(PartitionItem item:partition){
        switch (item.Method) {
          case VByte:{
            // 写入编码类型
            docOut.writeVInt(0);
            // 写入块的长度
            docOut.writeVInt(item.Partition.End-item.Partition.Start);
            int base = docIdBuffer.get(item.Partition.Start);
            // 写入base
            docOut.writeVInt(base);
            skipWriter.bufferSkip(docIdBuffer.get(item.Partition.End-1), item.Partition.End-item.Partition.Start, lastBlockPosFP, lastBlockPayFP, 0, 0);
            for(int i=item.Partition.Start+1;i<item.Partition.End;i++) {
              docOut.writeVInt(docIdBuffer.get(i)-base);
            }
            break;
          }
          case BitSet:{
            docOut.writeVInt(1);
            int base = docIdBuffer.get(item.Partition.Start);
            skipWriter.bufferSkip(docIdBuffer.get(item.Partition.End-1), item.Partition.End-item.Partition.Start, lastBlockPosFP, lastBlockPayFP, 0, 0);
            FixedBitSet bitSet = new FixedBitSet(docIdBuffer.get(item.Partition.End-1)-base);
            for(int i=item.Partition.Start+1;i<item.Partition.End;i++) {
              bitSet.set(docIdBuffer.get(i)-base-1);
            }
            byte[] bitSetData = longsToBytes(bitSet.getBits());
            docOut.writeVInt(bitSetData.length);
            docOut.writeVInt(base);
            docOut.writeBytes(bitSetData, bitSetData.length);
            break;
          }
        }
      }
    }

    final long lastPosBlockOffset;

    if (writePositions) {
      // totalTermFreq is just total number of positions(or payloads, or offsets)
      // associated with current term.
      assert state.totalTermFreq != -1;
      if (state.totalTermFreq > BLOCK_SIZE) {
        // record file offset for last pos in last block
        lastPosBlockOffset = posOut.getFilePointer() - posStartFP;
      } else {
        lastPosBlockOffset = -1;
      }
      if (posBufferUpto > 0) {       
        // TODO: should we send offsets/payloads to
        // .pay...?  seems wasteful (have to store extra
        // vLong for low (< BLOCK_SIZE) DF terms = vast vast
        // majority)

        // vInt encode the remaining positions/payloads/offsets:
        int lastPayloadLength = -1;  // force first payload length to be written
        int lastOffsetLength = -1;   // force first offset length to be written
        int payloadBytesReadUpto = 0;
        for(int i=0;i<posBufferUpto;i++) {
          final int posDelta = posDeltaBuffer[i];
          if (writePayloads) {
            final int payloadLength = payloadLengthBuffer[i];
            if (payloadLength != lastPayloadLength) {
              lastPayloadLength = payloadLength;
              posOut.writeVInt((posDelta<<1)|1);
              posOut.writeVInt(payloadLength);
            } else {
              posOut.writeVInt(posDelta<<1);
            }

            if (payloadLength != 0) {
              posOut.writeBytes(payloadBytes, payloadBytesReadUpto, payloadLength);
              payloadBytesReadUpto += payloadLength;
            }
          } else {
            posOut.writeVInt(posDelta);
          }

          if (writeOffsets) {
            int delta = offsetStartDeltaBuffer[i];
            int length = offsetLengthBuffer[i];
            if (length == lastOffsetLength) {
              posOut.writeVInt(delta << 1);
            } else {
              posOut.writeVInt(delta << 1 | 1);
              posOut.writeVInt(length);
              lastOffsetLength = length;
            }
          }
        }

        if (writePayloads) {
          assert payloadBytesReadUpto == payloadByteUpto;
          payloadByteUpto = 0;
        }
      }
    } else {
      lastPosBlockOffset = -1;
    }

    long skipOffset;
    if (docCount > 1) {
      skipOffset = skipWriter.writeSkip(docOut) - docStartFP;
    } else {
      skipOffset = -1;
    }

    state.docStartFP = docStartFP;
    state.posStartFP = posStartFP;
    state.payStartFP = payStartFP;
    state.singletonDocID = singletonDocID;
    state.skipOffset = skipOffset;
    state.lastPosBlockOffset = lastPosBlockOffset;
    posBufferUpto = 0;
    baseDocID = 0;
    docCount = 0;
    docIdBuffer.clear();
  }
  
  @Override
  public void encodeTerm(long[] longs, DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute) throws IOException {
    IntBlockTermState state = (IntBlockTermState)_state;
    if (absolute) {
      lastState = emptyState;
    }
    longs[0] = state.docStartFP - lastState.docStartFP;
    if (writePositions) {
      longs[1] = state.posStartFP - lastState.posStartFP;
      if (writePayloads || writeOffsets) {
        longs[2] = state.payStartFP - lastState.payStartFP;
      }
    }
    if (state.singletonDocID != -1) {
      out.writeVInt(state.singletonDocID);
    }
    if (writePositions) {
      if (state.lastPosBlockOffset != -1) {
        out.writeVLong(state.lastPosBlockOffset);
      }
    }
    if (state.skipOffset != -1) {
      out.writeVLong(state.skipOffset);
    }
    lastState = state;
  }

  @Override
  public void close() throws IOException {
    // TODO: add a finish() at least to PushBase? DV too...?
    boolean success = false;
    try {
      if (docOut != null) {
        CodecUtil.writeFooter(docOut);
      }
      if (posOut != null) {
        CodecUtil.writeFooter(posOut);
      }
      if (payOut != null) {
        CodecUtil.writeFooter(payOut);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(docOut, posOut, payOut);
      } else {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
      docOut = posOut = payOut = null;
    }
  }
}

