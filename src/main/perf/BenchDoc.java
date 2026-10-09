package perf;

import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Calendar;
import java.util.Locale;

import static perf.LineFileDocs.VECTOR_FIELD_NAME;

/**
 * Document class that will serve as a data model as well as carrying the actual field values,
 * for historic reason only one instance will be created and shared within one thread during the
 * whole indexing process, and there is no implicit reset when finishing indexing the previous
 * document and advancing to next document, which means if a field is set a value only once on
 * the first document, that value will be shared for all documents if no one ever touches that field
 * after the initialization.
 */
public class BenchDoc {
    private final Document luceneDoc; // reusable lucene Document instance
    private final Field titleTokenized;
    private final Field title;
    private final Field month;
    private final Field dayOfYear;
    private final BinaryDocValuesField titleBDV;
    private final Field lastMod;
    private final Field body;
    private final Field id;
    private final Field idPoint;
    private final Field idDV;
    private final Field date;
    private final Field randomLabel;
    private final Field lastModSkipper;
    private final Field monthSkipper;
    private final Field dayOfYearSkipper;
    private final Field titleSkipper;

    private final Field group100Field;
    private final Field group100KField;
    private final Field group10KField;
    private final Field group1MField;
    private final Field groupBlockField;
    private final Field groupEndField;

    //final NumericDocValuesField dateMSec;
    //final LongField rand;
    private final Field timeSec;
    // Necessary for "old style" wiki line files:
    static final DateTimeFormatter dateParser = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("dd-MMM-yyyy HH:mm:ss")
            .optionalStart()
            .appendPattern(".SSS")
            .optionalEnd()
            .toFormatter(Locale.US);
    private final KnnFloatVectorField floatVectorField;
    private final KnnByteVectorField byteVectorField;

    private final Calendar dateCal = Calendar.getInstance();

    BenchDoc(
            boolean storeBody, boolean tvsBody, boolean bodyPostingsOffsets, boolean addDVFields,
            boolean addDVSkippers, int vectorDimension, VectorEncoding vectorEncoding, boolean addGroupFields) {
        luceneDoc = new Document();

        if (addDVFields == false) {
            title = new StringField("title", "", Field.Store.NO);
        } else {
            title = new KeywordField("title", "", Field.Store.NO);
        }
        luceneDoc.add(title);

        if (addDVFields) {
            titleBDV = new BinaryDocValuesField("titleBDV", new BytesRef());
            luceneDoc.add(titleBDV);

            lastMod = new LongField("lastMod", -1, Field.Store.NO);
            luceneDoc.add(lastMod);

            month = new KeywordField("month", "", Field.Store.NO);
            luceneDoc.add(month);

            dayOfYear = new IntField("dayOfYear", 0, Field.Store.NO);
            luceneDoc.add(dayOfYear);

            idDV = new NumericDocValuesField("id", 0);
            luceneDoc.add(idDV);

            if (addDVSkippers) {
                lastModSkipper = NumericDocValuesField.indexedField("lastMod_skipper", -1);
                luceneDoc.add(lastModSkipper);

                monthSkipper = SortedDocValuesField.indexedField("month_skipper", new BytesRef(""));
                luceneDoc.add(monthSkipper);

                dayOfYearSkipper = NumericDocValuesField.indexedField("dayOfYear_skipper", 0);
                luceneDoc.add(dayOfYearSkipper);

                titleSkipper = SortedDocValuesField.indexedField("title_skipper", new BytesRef(""));
                luceneDoc.add(titleSkipper);
            } else {
                lastModSkipper = null;
                monthSkipper = null;
                dayOfYearSkipper = null;
                titleSkipper = null;
            }
        } else {
            titleBDV = null;
            lastMod = null;
            month = null;
            dayOfYear = null;
            idDV = null;
            lastModSkipper = null;
            monthSkipper = null;
            dayOfYearSkipper = null;
            titleSkipper = null;
        }

        titleTokenized = new TextField("titleTokenized", "", Field.Store.YES);
        luceneDoc.add(titleTokenized);

        FieldType bodyFieldType = new FieldType(TextField.TYPE_NOT_STORED);
        if (storeBody) {
            bodyFieldType.setStored(true);
        }

        if (tvsBody) {
            bodyFieldType.setStoreTermVectors(true);
            bodyFieldType.setStoreTermVectorOffsets(true);
            bodyFieldType.setStoreTermVectorPositions(true);
        }

        if (bodyPostingsOffsets) {
            bodyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        }

        body = new Field("body", "", bodyFieldType);
        luceneDoc.add(body);

        randomLabel = new StringField("randomLabel", "", Field.Store.NO);
        luceneDoc.add(randomLabel);

        id = new StringField("id", "", Field.Store.YES);
        luceneDoc.add(id);

        idPoint = new IntPoint("id", 0);
        luceneDoc.add(idPoint);

        date = new StringField("date", "", Field.Store.YES);
        luceneDoc.add(date);

        //dateMSec = new NumericDocValuesField("datenum", 0L);
        //doc.add(dateMSec);

        //rand = new LongField("rand", 0L, Field.Store.NO);
        //doc.add(rand);

        timeSec = new IntPoint("timesecnum", 0);
        luceneDoc.add(timeSec);

        if (vectorDimension > 0) {
            if (vectorEncoding == VectorEncoding.FLOAT32) {
                floatVectorField = new KnnFloatVectorField(VECTOR_FIELD_NAME, new float[vectorDimension], VectorSimilarityFunction.DOT_PRODUCT);
                luceneDoc.add(floatVectorField);
                byteVectorField = null;
            } else {
                byteVectorField = new KnnByteVectorField(VECTOR_FIELD_NAME, new byte[vectorDimension], VectorSimilarityFunction.DOT_PRODUCT);
                luceneDoc.add(byteVectorField);
                floatVectorField = null;
            }
        } else {
            floatVectorField = null;
            byteVectorField = null;
        }

        if (addGroupFields) {
            group100Field = new SortedDocValuesField("group100", new BytesRef());
            luceneDoc.add(group100Field);
            group10KField = new SortedDocValuesField("group10K", new BytesRef());
            luceneDoc.add(group10KField);
            group100KField = new SortedDocValuesField("group100K", new BytesRef());
            luceneDoc.add(group100KField);
            group1MField = new SortedDocValuesField("group1M", new BytesRef());
            luceneDoc.add(group1MField);
            groupBlockField = new SortedDocValuesField("groupblock", new BytesRef());
            luceneDoc.add(groupBlockField);
            // Binary marker field: this field should be ONLY included in the doc that is the end of group
            // so we don't add it here
            groupEndField = new StringField("groupend", "x", Field.Store.NO);
        } else {
            group100Field = null;
            group100KField = null;
            group10KField = null;
            group1MField = null;
            groupBlockField = null;
            groupEndField = null;
        }
    }

    public Document getLuceneDoc() {
        return luceneDoc;
    }

    public Calendar getDateCal() {
        return dateCal;
    }

    public boolean hasFloatVectorField() {
        return floatVectorField != null;
    }

    public boolean hasByteVectorField() {
        return byteVectorField != null;
    }

    public float[] getFloatVectorValue() {
        return floatVectorField.vectorValue();
    }

    public byte[] getByteVectorValue() {
        return byteVectorField.vectorValue();
    }

    public void setFloatVectorValue(float[] value) {
        if (floatVectorField != null) {
            floatVectorField.setVectorValue(value);
        }
    }

    public void setByteVectorValue(byte[] value) {
        if (byteVectorField != null) {
            byteVectorField.setVectorValue(value);
        }
    }

    public void setTitle(String value) {
        title.setStringValue(value);
    }

    public void setBody(String value) {
        body.setStringValue(value);
    }

    public void setTitleTokenized(String value) {
        titleTokenized.setStringValue(value);
    }

    public void setIdString(String value) {
        id.setStringValue(value);
    }

    public String getIdString() {
        return id.stringValue();
    }

    public void setDate(String value) {
        date.setStringValue(value);
    }

    public void setRandomLabel(String value) {
        randomLabel.setStringValue(value);
    }

    public void setTitleBDV(BytesRef value) {
        if (titleBDV != null) {
            titleBDV.setBytesValue(value);
        }
    }

    public void setMonth(String value) {
        if (month != null) {
            month.setStringValue(value);
        }
    }

    public void setDayOfYear(int value) {
        if (dayOfYear != null) {
            dayOfYear.setIntValue(value);
        }
    }

    public void setIdDV(long value) {
        if (idDV != null) {
            idDV.setLongValue(value);
        }
    }

    public void setMonthSkipper(BytesRef value) {
        if (monthSkipper != null) {
            monthSkipper.setBytesValue(value);
        }
    }

    public void setDayOfYearSkipper(long value) {
        if (dayOfYearSkipper != null) {
            dayOfYearSkipper.setLongValue(value);
        }
    }

    public void setTitleSkipper(BytesRef value) {
        if (titleSkipper != null) {
            titleSkipper.setBytesValue(value);
        }
    }

    public void setIdPoint(int value) {
        idPoint.setIntValue(value);
    }

    public void setLastMod(long value) {
        if (lastMod != null) {
            lastMod.setLongValue(value);
        }
    }

    public void setLastModSkipper(long value) {
        if (lastModSkipper != null) {
            lastModSkipper.setLongValue(value);
        }
    }

    public void setTimeSec(int value) {
        timeSec.setIntValue(value);
    }

    public void setGroupBlockField(BytesRef value) {
        groupBlockField.setBytesValue(value);
    }

    public void setGroup100Field(BytesRef value) {
        group100Field.setBytesValue(value);
    }

    public void setGroup10KField(BytesRef value) {
        group10KField.setBytesValue(value);
    }

    public void setGroup100KField(BytesRef value) {
        group100KField.setBytesValue(value);
    }

    public void setGroup1MField(BytesRef value) {
        group1MField.setBytesValue(value);
    }

    public void includeGroupEndField() {
        luceneDoc.add(groupEndField);
    }

    public void removeGroupEndField() {
        luceneDoc.removeField("groupend");
    }
}
