private class MapFiller implements Runnable {
    private final int stride;
    private final int offset;

    public MapFiller(int i, int length) {
        this.offset = i;
        this.stride = length;
    }

    @Override
    public void run() {
        for(int i = offset; i < input.length; i += stride) {
            map.put(input[i].get(in_group), input[i], agg, in_group, in_agg, out_group, out_agg);
        }
    }
}
}