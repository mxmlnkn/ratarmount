from qtpy.QtCore import Qt
from qtpy.QtWidgets import QLabel


class HoverLabel(QLabel):
    def __init__(self, image, hover_image, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._normal = image
        self._hover = hover_image

        self.setPixmap(self._normal)
        self.setAttribute(Qt.WA_Hover, True)

    def enterEvent(self, event):
        self.setPixmap(self._hover)
        super().enterEvent(event)

    def leaveEvent(self, event):
        self.setPixmap(self._normal)
        super().leaveEvent(event)
