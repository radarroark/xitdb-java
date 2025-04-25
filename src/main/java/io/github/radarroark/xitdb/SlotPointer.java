package io.github.radarroark.xitdb;

public record SlotPointer(Long position, Slot slot) {
    public SlotPointer withSlot(Slot slot) {
        return new SlotPointer(this.position, slot);
    }
}
