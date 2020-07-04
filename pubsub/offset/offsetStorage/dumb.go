package offsetStorage

type dumbOffsetStorage struct {
	offsets map[*SubscriberGroup]int
}

func (d *dumbOffsetStorage) GetLatest(key *SubscriberGroup) (int, error) {
	offset, ok := d.offsets[key]
	if !ok {
		return 0, SubscriberGroupNotFound{SubscriberGroup: key}
	}
	return offset, nil
}

func (d *dumbOffsetStorage) Store(key *SubscriberGroup, value int) error {
	d.offsets[key] = value
	return nil
}
